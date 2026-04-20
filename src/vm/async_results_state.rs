use crate::service_protocol::messages::{CombinatorType, Future};
use crate::service_protocol::{Notification, NotificationId, NotificationResult};
use crate::{NotificationHandle, UnresolvedFuture, CANCEL_NOTIFICATION_HANDLE};
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct AsyncResultsState {
    to_process: VecDeque<Notification>,
    ready: HashMap<NotificationId, NotificationResult>,

    handle_mapping: HashMap<NotificationHandle, NotificationId>,
    next_notification_handle: NotificationHandle,
}

impl Default for AsyncResultsState {
    fn default() -> Self {
        Self {
            to_process: Default::default(),
            ready: Default::default(),

            // First 15 are reserved for built-in signals!
            handle_mapping: HashMap::from([(
                CANCEL_NOTIFICATION_HANDLE,
                NotificationId::SignalId(1),
            )]),
            next_notification_handle: NotificationHandle(17),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub(crate) enum ResolveFutureResult {
    // The shared core has enough information to unblock the SDK, allowing it to take notifications.
    AnyCompleted,
    // The shared core needs some external input to make progress on this future.
    WaitExternalInput(UnresolvedFuture),
}

impl AsyncResultsState {
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.journal.notification.id = ?notification.id,
        ),
        ret
    )]
    pub(crate) fn enqueue(&mut self, notification: Notification) {
        self.to_process.push_back(notification);
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.journal.notification.id = ?notification.id,
        ),
        ret
    )]
    pub(crate) fn insert_ready(&mut self, notification: Notification) {
        self.ready.insert(notification.id, notification.result);
    }

    pub(crate) fn create_handle_mapping(
        &mut self,
        notification_id: NotificationId,
    ) -> NotificationHandle {
        let assigned_handle = self.next_notification_handle;
        self.next_notification_handle.0 += 1;
        self.handle_mapping.insert(assigned_handle, notification_id);
        assigned_handle
    }

    #[instrument(level = "trace", skip(self), ret)]
    pub(crate) fn try_resolve_future(
        &mut self,
        mut unresolved_future: UnresolvedFuture,
    ) -> ResolveFutureResult {
        // A bit of theory on future resolution.
        //
        // ## What is a future?
        // A Future represents one or more results the SDK is awaiting before resuming user code.
        //
        // Futures are modeled as a tree: leaves reference Notifications in the restate service-protocol,
        // while intermediate nodes are combinators that compose child futures.
        //
        // A future at any point in time can be in any of these 3 states:
        // * PENDING (initial state)
        // * SUCCEEDED (completed with success)
        // * FAILED (completed with failure)
        //
        // Once in SUCCEEDED or FAILED, the Future is immutable, that is it cannot change state.
        //
        // In the data model we have several types of combinators:
        // * SINGLE -> Resolve as soon as the operation is completed
        // * FIRST_COMPLETED -> Resolve as soon as any one child future completes with success, or with failure (same as JS Promise.race).
        // * ALL_COMPLETED -> Wait for every child to complete, regardless of success or failure (same as JS Promise.allSettled).
        // * FIRST_SUCCEEDED_OR_ALL_FAILED -> Resolve on the first success; fail only if all children fail (same as JS Promise.any).
        // * ALL_SUCCEEDED_OR_FIRST_FAILED -> Resolve when all children succeed; short-circuit on the first failure (same as JS Promise.all).
        // * UNKNOWN -> Unknown combinator made of 1 or more children futures.
        //              The SDK uses this for futures that are not representable in any of the above types.
        //              A notable example if RestatePromise.map in Javascript or Java SDK
        //
        // Implementation note: for performance reasons, the nested combinators use Vec to store children nodes, but they're conceptually sets.
        // Duplicates should not influence the normalization, nor the wake-up algorithm.
        //
        // ## Normalization algorithm
        // Invariant: No UNKNOWN future appears as a child of FIRST_SUCCEEDED_OR_ALL_FAILED or ALL_SUCCEEDED_OR_FIRST_FAILED.
        //   UNKNOWN is fine inside FIRST_COMPLETED, ALL_COMPLETED, and other UNKNOWN nodes,
        //   because those only need completed/pending status, not success/failure.
        // Algorithm:
        // * Recurse into nodes, and for each node:
        //     If the node is a FIRST_SUCCEEDED_OR_ALL_FAILED or ALL_SUCCEEDED_OR_FIRST_FAILED,
        //     and it has an Unknown node nested in its subtree:
        //       1. Extract the Unknown's children and save them in extracted_children
        //       2. Remove the Unknown node from the parent
        //       3. For each extracted child, recurse the algorithm. If the child is also UNKNOWN, flatten it recursively
        //       4. Add the remaining extracted_children to unknown_nodes
        // * At the end of the algorithm:
        //     If there are unknown_nodes:
        //       Substitute the root of the tree with an UNKNOWN node containing the current root of the tree + the unknown_nodes
        //
        // # Wake up algorithm
        // Theorem: Given as input the normalized tree and the list of notifications, and whether they are succeeded/failed,
        //          it is possible to establish whether the invocation can resume or not.
        // Proof by induction:
        //   Given a SINGLE future, the invocation can resume once the future is completed.
        //   Given a UNKNOWN future, the invocation can resume once any of the children are completed.
        //   Given a FIRST_COMPLETED future, the invocation can resume once any of the children are completed.
        //      UNKNOWN children are fine here: completed/pending can always be determined.
        //   Given an ALL_COMPLETED future, the invocation can resume when all the children are completed.
        //      UNKNOWN children are fine here: completed/pending can always be determined.
        //   Given a FIRST_SUCCEEDED_OR_ALL_FAILED future, the invocation can resume when either any of the children succeeded, or all the children failed.
        //      Because no children can be UNKNOWN (normalization invariant), success/failure is always determinable.
        //   Given an ALL_SUCCEEDED_OR_FIRST_FAILED future, the invocation can resume when either all the children succeeded, or any of the children failed.
        //      Because no children can be UNKNOWN (normalization invariant), success/failure is always determinable.
        //   Recursion
        //   QED
        //
        // # SDK and restate-server expectations
        // The SDK will try to normalize and resolve the nodes of the tree it can resolve.
        //
        // If the SDK can fully resolve the future with the local information, it will unblock the user code and move on
        // If the SDK cannot resolve the future, it will shave off the tree the Completed nodes, and propagate back the remaining part of the tree.
        //      Propagation will happen depending on the situation, via AwaitingOnMessage or SuspensionMessage.
        //
        // # About this function
        // try_resolve_future first normalizes the future, then loops:
        // * If the future can be resolved against the current `ready` state, return AnyCompleted
        //   and let the SDK consume the completed notifications.
        // * Otherwise, pop the next notification from `to_process` and retry.
        //   If the queue is empty, the only way to resolve the future is either read more input, or execute a pending ctx.run.
        //
        // The loop shortcircuits when some of the intermediate combinator nodes complete (see _try_resolve_future).
        // The shared core resolves the tree by reading from `ready` non-destructively, so a single
        // notification can satisfy multiple subtrees (e.g. a shared handle across two races). But the
        // SDK consumes notifications destructively via take_notification: once a handle is taken, it
        // is no longer visible. That means the SDK — not the shared core — ultimately decides which
        // notification value resolves each combinator, and the order in which the SDK observes completed
        // subtrees is part of the user-visible semantics!
        //
        // Wanna understand this better? In _try_resolve_future change all the Err to Ok, then run the unit tests below.

        unresolved_future.normalize();
        loop {
            let reduce_future_res = self._try_resolve_future(&mut unresolved_future);

            match reduce_future_res {
                Ok(handle_state) if handle_state.is_completed() => {
                    // Future is completed!
                    return ResolveFutureResult::AnyCompleted;
                }
                Err(_) => {
                    // Some of the nested combinator made progress, and shortcircuited the rest of the resolution.
                    // We need to give chance to the SDK to consume the new notifications.
                    return ResolveFutureResult::AnyCompleted;
                }
                Ok(_) => {
                    // HandleState is not completed

                    // If we pop some element from the notification queue, we can try to resolve the future again.
                    // Otherwise, the only possible way to make progress now is either reading from input stream, or running a ctx.run if any.
                    if !self.pop_notification_queue() {
                        return ResolveFutureResult::WaitExternalInput(unresolved_future);
                    }
                }
            }
        }
    }

    // Err() is used to resolve a future, but also "shortcircuit" the reduction process when "one at the time semantics" are required.
    fn _try_resolve_future(
        &self,
        unresolved_future: &mut UnresolvedFuture,
    ) -> Result<HandleState, HandleState> {
        match unresolved_future {
            UnresolvedFuture::Single(h) => Ok(self.resolve_handle_state(*h)),
            UnresolvedFuture::FirstCompleted(futures) | UnresolvedFuture::Unknown(futures) => {
                let mut any_completed = false;
                for fut in futures.iter_mut() {
                    if self._try_resolve_future(fut)?.is_completed() {
                        any_completed = true;
                        break;
                    }
                }

                // Resolve on any child completion (success or failure)
                if any_completed {
                    futures.clear();
                    // First completed short-circuits!
                    Err(HandleState::Succeeded)
                } else {
                    Ok(HandleState::Pending)
                }
            }
            UnresolvedFuture::AllCompleted(futures) => {
                // Wait for every child to complete
                let mut i = 0;
                while i < futures.len() {
                    if self._try_resolve_future(&mut futures[i])?.is_completed() {
                        futures.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if futures.is_empty() {
                    Ok(HandleState::Succeeded)
                } else {
                    Ok(HandleState::Pending)
                }
            }
            UnresolvedFuture::FirstSucceededOrAllFailed(futures) => {
                // First success wins; fail only if all fail
                let mut i = 0;
                while i < futures.len() {
                    let state = self._try_resolve_future(&mut futures[i])?;
                    if state == HandleState::Succeeded {
                        futures.clear();
                        return Err(HandleState::Succeeded);
                    } else if state == HandleState::Failed {
                        futures.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if futures.is_empty() {
                    Ok(HandleState::Failed)
                } else {
                    Ok(HandleState::Pending)
                }
            }
            UnresolvedFuture::AllSucceededOrFirstFailed(futures) => {
                // All must succeed; first failure short-circuits
                let mut i = 0;
                while i < futures.len() {
                    let state = self._try_resolve_future(&mut futures[i])?;
                    if state == HandleState::Failed {
                        futures.clear();
                        return Err(HandleState::Failed);
                    } else if state == HandleState::Succeeded {
                        futures.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if futures.is_empty() {
                    Ok(HandleState::Succeeded)
                } else {
                    Ok(HandleState::Pending)
                }
            }
        }
    }

    // Returns false if there's no more to_process
    fn pop_notification_queue(&mut self) -> bool {
        if let Some(notif) = self.to_process.pop_front() {
            self.ready.insert(notif.id, notif.result);
            true
        } else {
            false
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.shared_core.notification.handle = ?handle,
        ),
        ret
    )]
    pub(crate) fn is_handle_completed(&self, handle: NotificationHandle) -> bool {
        self.handle_mapping
            .get(&handle)
            .is_none_or(|id| self.ready.contains_key(id))
    }

    fn resolve_handle_state(&self, handle: NotificationHandle) -> HandleState {
        match self
            .handle_mapping
            .get(&handle)
            .and_then(|id| self.ready.get(id))
        {
            Some(notif) if notif.is_failure() => HandleState::Failed,
            Some(_) => HandleState::Succeeded,
            None => HandleState::Pending,
        }
    }

    pub(crate) fn non_deterministic_find_id(&self, id: &NotificationId) -> bool {
        if self.ready.contains_key(id) {
            return true;
        }
        self.to_process.iter().any(|notif| notif.id == *id)
    }

    pub(crate) fn resolve_notification_handles(
        &self,
        handles: &[NotificationHandle],
    ) -> HashSet<NotificationId> {
        handles
            .iter()
            .filter_map(|h| self.handle_mapping.get(h).cloned())
            .collect()
    }

    /// Convert an [`UnresolvedFuture`] tree to the wire-format [`Future`] message.
    ///
    /// Each variant maps 1:1 to a Future message. `Single` children are inlined
    /// into the parent's `waiting_*` fields. All other children (including `Unknown`)
    /// become nested `Future` messages.
    pub(crate) fn resolve_unresolved_future(&self, unresolved_future: UnresolvedFuture) -> Future {
        let mut future = Future::default();

        let children = match unresolved_future {
            UnresolvedFuture::Single(handle) => {
                future.combinator_type = CombinatorType::FirstCompleted as i32;
                self.push_handle(&mut future, &handle);
                return future;
            }
            UnresolvedFuture::Unknown(c) => c,
            UnresolvedFuture::FirstCompleted(c) => {
                future.combinator_type = CombinatorType::FirstCompleted as i32;
                c
            }
            UnresolvedFuture::AllCompleted(c) => {
                future.combinator_type = CombinatorType::AllCompleted as i32;
                c
            }
            UnresolvedFuture::FirstSucceededOrAllFailed(c) => {
                future.combinator_type = CombinatorType::FirstSucceededOrAllFailed as i32;
                c
            }
            UnresolvedFuture::AllSucceededOrFirstFailed(c) => {
                future.combinator_type = CombinatorType::AllSucceededOrFirstFailed as i32;
                c
            }
        };

        for child in children {
            match child {
                UnresolvedFuture::Single(handle) => self.push_handle(&mut future, &handle),
                other => future
                    .nested_futures
                    .push(self.resolve_unresolved_future(other)),
            }
        }
        future
    }

    /// Add a handle's notification ID to the appropriate `waiting_*` field.
    fn push_handle(&self, future: &mut Future, handle: &NotificationHandle) {
        match self.handle_mapping.get(handle) {
            Some(NotificationId::CompletionId(id)) => future.waiting_completions.push(*id),
            Some(NotificationId::SignalId(id)) => future.waiting_signals.push(*id),
            Some(NotificationId::SignalName(name)) => {
                future.waiting_named_signals.push(name.clone())
            }
            None => {}
        }
    }

    pub(crate) fn must_resolve_notification_handle(
        &self,
        handle: &NotificationHandle,
    ) -> NotificationId {
        self.handle_mapping
            .get(handle)
            .expect("If there is an handle, there must be a corresponding id")
            .clone()
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.shared_core.notification.handle = ?handle,
        ),
        ret
    )]
    pub(crate) fn take_handle(&mut self, handle: NotificationHandle) -> Option<NotificationResult> {
        let id = self.handle_mapping.get(&handle)?;
        if let Some(res) = self.ready.remove(id) {
            self.handle_mapping.remove(&handle);
            Some(res)
        } else {
            None
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.shared_core.notification.handle = ?handle,
        ),
        ret
    )]
    pub(crate) fn copy_handle(&mut self, handle: NotificationHandle) -> Option<NotificationResult> {
        self.ready.get(self.handle_mapping.get(&handle)?).cloned()
    }
}

#[derive(Debug, Eq, PartialEq)]
enum HandleState {
    Succeeded,
    Failed,
    Pending,
}

impl HandleState {
    fn is_completed(&self) -> bool {
        matches!(self, HandleState::Succeeded | HandleState::Failed)
    }
}

impl UnresolvedFuture {
    fn normalize(&mut self) {
        let mut unknown_nodes = vec![];
        Self::normalize_inner(self, false, &mut unknown_nodes);

        if !unknown_nodes.is_empty() {
            let current_root = std::mem::replace(self, UnresolvedFuture::Unknown(vec![]));
            // Only include the root if it's not an empty sentinel
            if !current_root.is_empty() {
                unknown_nodes.insert(0, current_root);
            }
            *self = UnresolvedFuture::Unknown(unknown_nodes);
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, UnresolvedFuture::Unknown(c) if c.is_empty())
    }

    /// Recurse into a node, extracting Unknowns when inside a fsaf/asff context.
    /// `extract` is true when we're inside a fsaf/asff subtree and must pull out any Unknown.
    fn normalize_inner(
        fut: &mut UnresolvedFuture,
        extract: bool,
        unknown_nodes: &mut Vec<UnresolvedFuture>,
    ) {
        match fut {
            UnresolvedFuture::Single(_) => return,
            UnresolvedFuture::Unknown(_) => {
                // If we're in extract mode, the caller handles extraction.
                // Otherwise, recurse into Unknown's children (they may contain fsaf/asff).
                if !extract {
                    if let UnresolvedFuture::Unknown(children) = fut {
                        for child in children.iter_mut() {
                            Self::normalize_inner(child, false, unknown_nodes);
                        }
                    }
                }
                return;
            }
            _ => {}
        }

        // Determine if THIS node triggers extraction for its children
        let needs_extraction = matches!(
            fut,
            UnresolvedFuture::FirstSucceededOrAllFailed(_)
                | UnresolvedFuture::AllSucceededOrFirstFailed(_)
        );
        // Children inherit extract mode from parent, or enter it if this node is fsaf/asff
        let child_extract = extract || needs_extraction;

        let children = match fut {
            UnresolvedFuture::FirstCompleted(c)
            | UnresolvedFuture::AllCompleted(c)
            | UnresolvedFuture::FirstSucceededOrAllFailed(c)
            | UnresolvedFuture::AllSucceededOrFirstFailed(c) => c,
            _ => unreachable!(),
        };

        // Recurse into non-Unknown children
        for child in children.iter_mut() {
            Self::normalize_inner(child, child_extract, unknown_nodes);
        }

        // Extract Unknown children if we're in extract mode
        if child_extract {
            let mut i = 0;
            while i < children.len() {
                if matches!(children[i], UnresolvedFuture::Unknown(_)) {
                    let unknown = children.swap_remove(i);
                    if let UnresolvedFuture::Unknown(extracted) = unknown {
                        for gc in extracted {
                            Self::collect_from_unknown_child(gc, unknown_nodes);
                        }
                    }
                } else {
                    i += 1;
                }
            }
        }

        // Simplify degenerate combinators
        let children = match fut {
            UnresolvedFuture::FirstCompleted(c)
            | UnresolvedFuture::AllCompleted(c)
            | UnresolvedFuture::FirstSucceededOrAllFailed(c)
            | UnresolvedFuture::AllSucceededOrFirstFailed(c) => c,
            _ => return,
        };
        if children.len() <= 1 {
            *fut = children.pop().unwrap_or(UnresolvedFuture::Unknown(vec![]));
        }
    }

    /// Process a child extracted from an Unknown node.
    /// If the child is itself Unknown, flatten recursively.
    /// Otherwise, normalize it and add to unknown_nodes.
    fn collect_from_unknown_child(
        fut: UnresolvedFuture,
        unknown_nodes: &mut Vec<UnresolvedFuture>,
    ) {
        match fut {
            UnresolvedFuture::Unknown(children) => {
                for child in children {
                    Self::collect_from_unknown_child(child, unknown_nodes);
                }
            }
            mut other => {
                Self::normalize_inner(&mut other, false, unknown_nodes);
                if !other.is_empty() {
                    unknown_nodes.push(other);
                }
            }
        }
    }

    pub(crate) fn handles(&self) -> Vec<NotificationHandle> {
        let mut handles = vec![];
        match self {
            UnresolvedFuture::Single(h) => handles.push(*h),
            UnresolvedFuture::Unknown(inner)
            | UnresolvedFuture::FirstCompleted(inner)
            | UnresolvedFuture::AllCompleted(inner)
            | UnresolvedFuture::FirstSucceededOrAllFailed(inner)
            | UnresolvedFuture::AllSucceededOrFirstFailed(inner) => {
                for fut in inner {
                    handles.extend(fut.handles());
                }
            }
        };
        handles
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::ResolveFutureResult::*;
    use crate::service_protocol::messages::{Failure, Void};
    use crate::service_protocol::{Notification, NotificationId, NotificationResult};
    use crate::{
        all_completed, all_succeeded_or_first_failed, first_completed,
        first_succeeded_or_all_failed, unknown,
    };
    use googletest::prelude::*;
    use pastey::paste;

    // --- Helpers ---

    fn success(id: u32) -> Notification {
        Notification {
            id: NotificationId::CompletionId(id),
            result: NotificationResult::Void(Void {}),
        }
    }

    fn failure(id: u32) -> Notification {
        Notification {
            id: NotificationId::CompletionId(id),
            result: NotificationResult::Failure(Failure {
                code: 500,
                message: "fail".to_string(),
                ..Default::default()
            }),
        }
    }

    fn state(enqueued: impl IntoIterator<Item = Notification>) -> AsyncResultsState {
        let mut state = AsyncResultsState::default();
        for i in 1..=10 {
            state
                .handle_mapping
                .insert(i.into(), NotificationId::CompletionId(i));
        }
        for notification in enqueued {
            state.enqueue(notification);
        }
        state
    }

    fn handles(handles: impl IntoIterator<Item = (u32, NotificationId)>) -> AsyncResultsState {
        let mut state = AsyncResultsState::default();
        for (handle, id) in handles {
            state.handle_mapping.insert(handle.into(), id);
        }
        state
    }

    macro_rules! test_normalization {
        ($test_name:ident: $input:expr => $expected:expr) => {
            paste! {
                #[test]
                fn [<normalize_ $test_name>] () {
                    let mut fut: UnresolvedFuture = ($input).into();
                    fut.normalize();
                    assert_eq!(fut, $expected);
                }
            }
        };
    }

    macro_rules! test_try_future_resolve {
        ($test_name:ident: $state:expr, $input:expr => $expected:expr) => {
            paste! {
                #[test]
                fn [<try_resolve_ $test_name>] () {
                    let mut state = $state;
                    let fut: UnresolvedFuture = ($input).into();
                    assert_eq!(state.try_resolve_future(fut), $expected);
                }
            }
        };
    }

    macro_rules! test_convert_unresolved_future {
        ($test_name:ident: $state:expr, $input:expr => $expected:expr) => {
            paste! {
                #[test]
                fn [<unresolved_future_to_message_ $test_name>] () {
                    let state = $state;
                    let fut: UnresolvedFuture = ($input).into();
                    assert_that!(state.resolve_unresolved_future(fut), $expected);
                }
            }
        };
    }

    // ==================== Single ====================

    test_try_future_resolve!(single_succeeded:
        state([success(1)]), 1 => AnyCompleted);
    test_try_future_resolve!(single_failed:
        state([failure(1)]), 1 => AnyCompleted);
    test_try_future_resolve!(single_pending:
        state([]), 1 => WaitExternalInput(1.into()));

    // ==================== FirstCompleted ====================

    test_normalization!(nested_combinators_no_unknowns_is_noop:
        first_completed!(1, all_completed!(2, 3))
        => first_completed!(1, all_completed!(2, 3)));
    // Unknown inside first_completed is fine — no extraction needed
    test_normalization!(first_completed_with_unknown_is_noop:
        first_completed!(1, unknown!(2))
        => first_completed!(1, unknown!(2)));
    // Unknown wrapping a combinator inside first_completed — no extraction
    test_normalization!(first_completed_with_unknown_wrapping_combinator:
        first_completed!(unknown!(all_completed!(1, 2)), 3)
        => first_completed!(unknown!(all_completed!(1, 2)), 3));
    // first_completed with one Unknown child — collapses (1 child)
    test_normalization!(first_completed_unknown_with_nested_unknown_collapses:
        first_completed!(unknown!(all_completed!(1, unknown!(2))))
        => unknown!(all_completed!(1, unknown!(2))));

    test_try_future_resolve!(first_completed_none_ready:
        state([]), first_completed!(1, 2, 3)
        => WaitExternalInput(first_completed!(1, 2, 3)));
    test_try_future_resolve!(first_completed_one_succeeded:
        state([success(2)]), first_completed!(1, 2, 3)
        => AnyCompleted);
    // Resolves on any completion, even failure
    test_try_future_resolve!(first_completed_one_failed:
        state([failure(1)]), first_completed!(1, 2, 3)
        => AnyCompleted);
    // first_completed(1, unknown(2)) — unknown(2) completes
    test_try_future_resolve!(first_completed_with_unknown_resolves:
        state([success(2)]), first_completed!(1, unknown!(2))
        => AnyCompleted);
    // first_completed(unknown(all_completed(1, 2)), 3) — completing 3 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_resolves_on_leaf:
        state([success(3)]),
        first_completed!(unknown!(all_completed!(1, 2)), 3)
        => AnyCompleted);
    // Completing 1 alone doesn't resolve: unknown(all_completed(1,2)) needs both
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_partial_inner:
        state([success(1)]),
        first_completed!(unknown!(all_completed!(1, 2)), 3)
        => WaitExternalInput(first_completed!(unknown!(all_completed!(2)), 3)));
    // Completing 1 and 2 resolves
    test_try_future_resolve!(first_completed_unknown_wrapping_combinator_inner_done:
        state([success(1), success(2)]),
        first_completed!(unknown!(all_completed!(1, 2)), 3)
        => AnyCompleted);
    // Completing just 1 is NOT enough
    test_try_future_resolve!(deep_unknown_not_prematurely_resolved:
        state([success(1)]),
        first_completed!(unknown!(all_completed!(1, unknown!(2))))
        => WaitExternalInput(unknown!(all_completed!(unknown!(2)))));
    test_try_future_resolve!(deep_unknown_resolves_when_all_done:
        state([success(1), success(2)]),
        first_completed!(unknown!(all_completed!(1, unknown!(2))))
        => AnyCompleted);

    // ==================== AllCompleted ====================

    test_normalization!(no_unknowns_is_noop:
        all_completed!(1, 2) => all_completed!(1, 2));
    // Unknown inside all_completed is fine — no extraction
    test_normalization!(all_completed_with_unknown_is_noop:
        all_completed!(1, unknown!(2))
        => all_completed!(1, unknown!(2)));
    test_normalization!(all_completed_multiple_unknowns_is_noop:
        all_completed!(unknown!(1), unknown!(2))
        => all_completed!(unknown!(1), unknown!(2)));
    test_normalization!(all_completed_nested_unknown_is_noop:
        all_completed!(1, unknown!(unknown!(2)))
        => all_completed!(1, unknown!(unknown!(2))));

    test_try_future_resolve!(all_completed_none_ready:
        state([]), all_completed!(1, 2, 3)
        => WaitExternalInput(all_completed!(1, 2, 3)));
    test_try_future_resolve!(all_completed_partial:
        state([success(1), failure(3)]), all_completed!(1, 2, 3)
        => WaitExternalInput(all_completed!(2)));
    test_try_future_resolve!(all_completed_all_done:
        state([success(1), failure(2)]), all_completed!(1, 2)
        => AnyCompleted);
    // Handle 1 completes but unknown(2) still pending
    test_try_future_resolve!(all_completed_with_unknown_partial:
        state([success(1)]), all_completed!(1, unknown!(2))
        => WaitExternalInput(all_completed!(unknown!(2))));
    test_try_future_resolve!(all_completed_with_unknown_all_done:
        state([success(1), success(2)]), all_completed!(1, unknown!(2))
        => AnyCompleted);

    // ==================== FirstSucceededOrAllFailed ====================

    // fsaf extracts unknowns
    test_normalization!(fsaf_extracts_unknown:
        first_succeeded_or_all_failed!(1, unknown!(2))
        => unknown!(1, 2));
    test_normalization!(nested_unknown_inside_fsaf_asff:
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => unknown!(first_succeeded_or_all_failed!(1, 2), 3));
    // Unknown deep inside fsaf subtree gets extracted
    test_normalization!(fsaf_with_all_completed_containing_unknown:
        first_succeeded_or_all_failed!(1, all_completed!(2, unknown!(3)))
        => unknown!(first_succeeded_or_all_failed!(1, 2), 3));
    test_normalization!(fsaf_with_unknown_containing_asff:
        first_succeeded_or_all_failed!(1, unknown!(all_succeeded_or_first_failed!(2, 3)))
        => unknown!(1, all_succeeded_or_first_failed!(2, 3)));
    // Cascading extraction
    test_normalization!(fsaf_with_unknown_containing_asff_with_unknown:
        first_succeeded_or_all_failed!(1, unknown!(2, all_succeeded_or_first_failed!(3, unknown!(4))))
        => unknown!(1, 2, 4, 3));

    test_try_future_resolve!(first_succeeded_or_all_failed_none_ready:
        state([]), first_succeeded_or_all_failed!(1, 2, 3)
        => WaitExternalInput(first_succeeded_or_all_failed!(1, 2, 3)));
    test_try_future_resolve!(first_succeeded_or_all_failed_one_succeeded:
        state([success(2)]), first_succeeded_or_all_failed!(1, 2, 3)
        => AnyCompleted);
    // swap_remove changes order
    test_try_future_resolve!(first_succeeded_or_all_failed_some_failed_some_pending:
        state([failure(1)]), first_succeeded_or_all_failed!(1, 2, 3)
        => WaitExternalInput(first_succeeded_or_all_failed!(3, 2)));
    test_try_future_resolve!(first_succeeded_or_all_failed_all_failed:
        state([failure(1), failure(2)]), first_succeeded_or_all_failed!(1, 2)
        => AnyCompleted);

    // fsaf(1, asff(2, unknown(3))) → unknown(fsaf(1, 2), 3)
    test_try_future_resolve!(normalization_fsaf_asff_unknown_success:
        state([success(3)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => AnyCompleted);
    test_try_future_resolve!(normalization_fsaf_asff_unknown_failure:
        state([failure(3)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => AnyCompleted);
    // 1 fails → fsaf prunes it, fsaf(2) still pending. 3 still pending.
    test_try_future_resolve!(normalization_nested_fsaf_asff_partial:
        state([failure(1)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => WaitExternalInput(unknown!(first_succeeded_or_all_failed!(2), 3)));
    // fsaf(1, all_completed(2, unknown(3))) — deep extraction
    test_try_future_resolve!(normalization_fsaf_with_nested_unknown_in_all_completed:
        state([success(3)]),
        first_succeeded_or_all_failed!(1, all_completed!(2, unknown!(3)))
        => AnyCompleted);
    test_try_future_resolve!(normalization_fsaf_with_nested_unknown_in_all_completed_pending:
        state([]),
        first_succeeded_or_all_failed!(1, all_completed!(2, unknown!(3)))
        => WaitExternalInput(unknown!(first_succeeded_or_all_failed!(1, 2), 3)));
    // fsaf(1, unknown(asff(2, 3))) → unknown(1, asff(2, 3))
    test_try_future_resolve!(fsaf_unknown_asff_resolves_when_inner_done:
        state([success(2), success(3)]),
        first_succeeded_or_all_failed!(1, unknown!(all_succeeded_or_first_failed!(2, 3)))
        => AnyCompleted);
    test_try_future_resolve!(fsaf_unknown_asff_inner_failure_resolves:
        state([failure(2)]),
        first_succeeded_or_all_failed!(1, unknown!(all_succeeded_or_first_failed!(2, 3)))
        => AnyCompleted);
    test_try_future_resolve!(fsaf_unknown_asff_inner_partial:
        state([success(2)]),
        first_succeeded_or_all_failed!(1, unknown!(all_succeeded_or_first_failed!(2, 3)))
        => WaitExternalInput(unknown!(1, all_succeeded_or_first_failed!(3))));

    // ==================== AllSucceededOrFirstFailed ====================

    // asff extracts unknowns
    test_normalization!(asff_extracts_unknown:
        all_succeeded_or_first_failed!(1, unknown!(2))
        => unknown!(1, 2));
    test_normalization!(asff_with_unknown_containing_fsaf:
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => unknown!(1, 2, first_succeeded_or_all_failed!(3, 4)));
    // all_completed inside extracted unknown is kept as-is
    test_normalization!(asff_with_unknown_containing_all_completed_with_unknown:
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => unknown!(1, all_completed!(2, unknown!(3))));

    test_try_future_resolve!(all_succeeded_or_first_failed_none_ready:
        state([]), all_succeeded_or_first_failed!(1, 2, 3)
        => WaitExternalInput(all_succeeded_or_first_failed!(1, 2, 3)));
    test_try_future_resolve!(all_succeeded_or_first_failed_all_succeeded:
        state([success(1), success(2)]), all_succeeded_or_first_failed!(1, 2)
        => AnyCompleted);
    test_try_future_resolve!(all_succeeded_or_first_failed_one_failed:
        state([failure(2)]), all_succeeded_or_first_failed!(1, 2, 3)
        => AnyCompleted);
    // swap_remove changes order
    test_try_future_resolve!(all_succeeded_or_first_failed_some_succeeded_some_pending:
        state([success(1)]), all_succeeded_or_first_failed!(1, 2, 3)
        => WaitExternalInput(all_succeeded_or_first_failed!(3, 2)));
    // Inner failure propagates up
    test_try_future_resolve!(promise_all_short_circuits_on_nested_failure:
        state([failure(2)]),
        all_succeeded_or_first_failed!(all_succeeded_or_first_failed!(1, 2), 3)
        => AnyCompleted);

    // asff(1, unknown(2)) → unknown(1, 2). Failure of 2 wakes.
    test_try_future_resolve!(normalization_asff_extracts_unknown:
        state([failure(2)]),
        all_succeeded_or_first_failed!(1, unknown!(2))
        => AnyCompleted);
    // asff(1, unknown(2, fsaf(3, 4))) → unknown(1, 2, fsaf(3, 4))
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_leaf:
        state([success(1)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_inner_fsaf_success:
        state([success(3)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => AnyCompleted);
    // 3 fails but 4 pending → fsaf pending → nothing AnyCompleted
    test_try_future_resolve!(asff_unknown_fsaf_failure_doesnt_resolve:
        state([failure(3)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => WaitExternalInput(unknown!(1, 2, first_succeeded_or_all_failed!(4))));
    // Both 3 and 4 fail → fsaf AnyCompleted → unknown wakes
    test_try_future_resolve!(asff_unknown_fsaf_all_inner_fail:
        state([failure(3), failure(4)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_fsaf_pending:
        state([]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => WaitExternalInput(unknown!(1, 2, first_succeeded_or_all_failed!(3, 4))));
    // asff(1, unknown(all_completed(2, unknown(3)))) → unknown(1, all_completed(2, unknown(3)))
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_resolves_on_leaf:
        state([success(1)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_partial:
        state([success(2)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => WaitExternalInput(unknown!(1, all_completed!(unknown!(3)))));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_all_done:
        state([success(2), success(3)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => AnyCompleted);

    // ==================== Unknown ====================

    test_normalization!(single_is_noop:
        1 => 1.into());
    test_normalization!(unknown_at_root_is_noop:
        unknown!(1, 2) => unknown!(1, 2));
    // Deep: unknown inside asff inside unknown inside all_completed
    test_normalization!(deep_nested_asff_with_unknown:
        all_completed!(unknown!(all_succeeded_or_first_failed!(1, unknown!(2))))
        => unknown!(unknown!(1), 2));

    test_try_future_resolve!(unknown_none_ready:
        state([]), unknown!(1, 2)
        => WaitExternalInput(unknown!(1, 2)));
    test_try_future_resolve!(unknown_one_ready:
        state([success(2)]), unknown!(1, 2)
        => AnyCompleted);

    // ==================== Nested combinators ====================

    test_try_future_resolve!(nested_all_inside_first_completed:
        state([success(3)]), first_completed!(all_completed!(1, 2), 3)
        => AnyCompleted);
    test_try_future_resolve!(nested_first_completed_inside_all_partial:
        state([success(1)]),
        all_completed!(first_completed!(1, 2), first_completed!(3, 4))
        => AnyCompleted);
    test_try_future_resolve!(nested_first_completed_inside_all_complete:
        state([success(1), success(4)]),
        all_completed!(first_completed!(1, 2), first_completed!(3, 4))
        => AnyCompleted);
    test_try_future_resolve!(nested_asff_inside_all_partial:
        state([failure(1)]),
        all_completed!(all_succeeded_or_first_failed!(1, 2), first_completed!(3, 4))
        => AnyCompleted);
    test_try_future_resolve!(nested_fsaf_inside_all_partial:
        state([success(1)]),
        all_completed!(first_succeeded_or_all_failed!(1, 2), first_completed!(3, 4))
        => AnyCompleted);

    // ==================== Duplicated leaves and subtrees ====================

    test_try_future_resolve!(duplicated_leaf_in_all_completed:
        state([success(1)]), all_completed!(1, 1)
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_in_first_completed:
        state([success(1)]), first_completed!(1, 1, 2)
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_failure_in_promise_all:
        state([failure(1)]), all_succeeded_or_first_failed!(1, 1, 2)
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_success_in_promise_any:
        state([success(1)]), first_succeeded_or_all_failed!(1, 1)
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_across_nested_combinators:
        state([success(1)]),
        all_completed!(first_completed!(1, 2), first_completed!(1, 3))
        => AnyCompleted);
    test_try_future_resolve!(duplicated_subtree_all_succeeded:
        state([success(1), success(2)]),
        all_completed!(all_succeeded_or_first_failed!(1, 2), all_succeeded_or_first_failed!(1, 2))
        => AnyCompleted);
    test_try_future_resolve!(duplicated_subtree_with_failure:
        state([failure(1)]),
        all_completed!(all_succeeded_or_first_failed!(1, 2), all_succeeded_or_first_failed!(1, 2))
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_with_unknown_in_all_completed:
        state([success(1)]),
        all_completed!(1, unknown!(1, 2))
        => AnyCompleted);
    test_try_future_resolve!(duplicated_leaf_partial_resolution:
        state([success(1)]), all_completed!(1, 2, 1)
        => WaitExternalInput(all_completed!(2)));

    // ==================== Conversions to protocol future type ====================

    test_convert_unresolved_future!(single_completion:
        handles([(1, NotificationId::CompletionId(1))]),
        1
        => pat!(Future {
            waiting_completions: eq(&[1]),
            waiting_signals: empty(),
            waiting_named_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    test_convert_unresolved_future!(single_signal:
        handles([(1, NotificationId::SignalId(17))]),
        1
        => pat!(Future {
            waiting_completions: empty(),
            waiting_signals: eq(&[17]),
            waiting_named_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    test_convert_unresolved_future!(single_named_signal:
        handles([(1, NotificationId::SignalName("foo".to_string()))]),
        1
        => pat!(Future {
            waiting_completions: empty(),
            waiting_signals: empty(),
            waiting_named_signals: eq(&["foo".to_string()]),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // first_completed(1, 2) — both are completions, flattened into waiting_completions
    test_convert_unresolved_future!(first_completed_flat:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::CompletionId(2))]),
        first_completed!(1, 2)
        => pat!(Future {
            waiting_completions: unordered_elements_are![eq(1), eq(2)],
            waiting_signals: empty(),
            waiting_named_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // first_completed(completion, signal) — mixed types flattened
    test_convert_unresolved_future!(first_completed_mixed:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::SignalId(5))]),
        first_completed!(1, 2)
        => pat!(Future {
            waiting_completions: eq(&[1]),
            waiting_signals: eq(&[5]),
            waiting_named_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // all_completed(1, 2)
    test_convert_unresolved_future!(all_completed_flat:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::CompletionId(2))]),
        all_completed!(1, 2)
        => pat!(Future {
            waiting_completions: unordered_elements_are![eq(1), eq(2)],
            waiting_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::AllCompleted as i32)
        })
    );

    // fsaf(1, 2)
    test_convert_unresolved_future!(fsaf_flat:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::CompletionId(2))]),
        first_succeeded_or_all_failed!(1, 2)
        => pat!(Future {
            waiting_completions: unordered_elements_are![eq(1), eq(2)],
            waiting_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::FirstSucceededOrAllFailed as i32)
        })
    );

    // asff(1, 2)
    test_convert_unresolved_future!(asff_flat:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::CompletionId(2))]),
        all_succeeded_or_first_failed!(1, 2)
        => pat!(Future {
            waiting_completions: unordered_elements_are![eq(1), eq(2)],
            waiting_signals: empty(),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::AllSucceededOrFirstFailed as i32)
        })
    );

    // unknown(1, 2) — Unknown has no combinator_type, children flattened
    test_convert_unresolved_future!(unknown_flat:
        handles([(1, NotificationId::CompletionId(1)), (2, NotificationId::SignalId(5))]),
        unknown!(1, 2)
        => pat!(Future {
            waiting_completions: eq(&[1]),
            waiting_signals: eq(&[5]),
            nested_futures: empty(),
            combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
        })
    );

    // first_completed(1, all_completed(2, 3)) — nested combinator becomes nested Future
    test_convert_unresolved_future!(nested_combinator:
        handles([
            (1, NotificationId::CompletionId(1)),
            (2, NotificationId::CompletionId(2)),
            (3, NotificationId::CompletionId(3))
        ]),
        first_completed!(1, all_completed!(2, 3))
        => pat!(Future {
            waiting_completions: eq(&[1]),
            waiting_signals: empty(),
            nested_futures: elements_are![pat!(Future {
                waiting_completions: unordered_elements_are![eq(2), eq(3)],
                nested_futures: empty(),
                combinator_type: eq(CombinatorType::AllCompleted as i32)
            })],
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // first_completed(unknown(1, 2), 3) — unknown child preserved as nested Future
    test_convert_unresolved_future!(unknown_child_nested:
        handles([
            (1, NotificationId::CompletionId(1)),
            (2, NotificationId::CompletionId(2)),
            (3, NotificationId::CompletionId(3))
        ]),
        first_completed!(unknown!(1, 2), 3)
        => pat!(Future {
            waiting_completions: eq(&[3]),
            waiting_signals: empty(),
            nested_futures: elements_are![pat!(Future {
                waiting_completions: unordered_elements_are![eq(1), eq(2)],
                combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
            })],
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // first_completed(unknown(all_completed(1, 2)), 3) — unknown wrapping combinator:
    // unknown becomes nested Future, all_completed nested inside it
    test_convert_unresolved_future!(unknown_wrapping_combinator:
        handles([
            (1, NotificationId::CompletionId(1)),
            (2, NotificationId::CompletionId(2)),
            (3, NotificationId::CompletionId(3))
        ]),
        first_completed!(unknown!(all_completed!(1, 2)), 3)
        => pat!(Future {
            waiting_completions: eq(&[3]),
            waiting_signals: empty(),
            nested_futures: elements_are![pat!(Future {
                waiting_completions: empty(),
                nested_futures: elements_are![pat!(Future {
                    waiting_completions: unordered_elements_are![eq(1), eq(2)],
                    combinator_type: eq(CombinatorType::AllCompleted as i32)
                })],
                combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
            })],
            combinator_type: eq(CombinatorType::FirstCompleted as i32)
        })
    );

    // unknown(fsaf(1, 2), 3) — root unknown: fsaf nested, 3 inlined (Single)
    test_convert_unresolved_future!(unknown_root_with_nested_combinator:
        handles([
            (1, NotificationId::CompletionId(1)),
            (2, NotificationId::CompletionId(2)),
            (3, NotificationId::SignalId(5))
        ]),
        unknown!(first_succeeded_or_all_failed!(1, 2), 3)
        => pat!(Future {
            waiting_completions: empty(),
            waiting_signals: eq(&[5]),
            nested_futures: elements_are![pat!(Future {
                waiting_completions: unordered_elements_are![eq(1), eq(2)],
                combinator_type: eq(CombinatorType::FirstSucceededOrAllFailed as i32)
            })],
            combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
        })
    );

    // all_completed(1, unknown(2)) — unknown child preserved as nested Future
    test_convert_unresolved_future!(all_completed_with_unknown_child:
        handles([
            (1, NotificationId::CompletionId(1)),
            (2, NotificationId::SignalId(17))
        ]),
        all_completed!(1, unknown!(2))
        => pat!(Future {
            waiting_completions: eq(&[1]),
            waiting_signals: empty(),
            nested_futures: elements_are![pat!(Future {
                waiting_signals: eq(&[17]),
                combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
            })],
            combinator_type: eq(CombinatorType::AllCompleted as i32)
        })
    );
}
