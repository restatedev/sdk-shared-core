use crate::service_protocol::messages::{CombinatorType, Future};
use crate::service_protocol::{CompletionId, Notification, NotificationId, NotificationResult};
use crate::vm::errors::BadProposeRunCompletionAck;
use crate::{
    Error, NotificationHandle, SignalDelivery, UnresolvedFuture, CANCEL_NOTIFICATION_HANDLE,
};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct AsyncResultsState {
    to_process: VecDeque<Notification>,
    // Deliveries ready to be taken, in arrival order per notification id.
    // Entries are never empty: the entry is removed when its last delivery is taken.
    ready: HashMap<NotificationId, VecDeque<NotificationResult>>,
    signal_delivery: SignalDelivery,

    // We cache the results of the run completions executed during this attempt.
    // From protocol >= v7
    cached_run_completions: HashMap<CompletionId, NotificationResult>,

    handle_mapping: HashMap<NotificationHandle, NotificationId>,
    next_notification_handle: NotificationHandle,
}

impl Default for AsyncResultsState {
    fn default() -> Self {
        Self::new(SignalDelivery::default())
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
    pub(crate) fn new(signal_delivery: SignalDelivery) -> Self {
        Self {
            to_process: Default::default(),
            ready: Default::default(),
            signal_delivery,

            cached_run_completions: Default::default(),

            // First 15 are reserved for built-in signals!
            handle_mapping: HashMap::from([(
                CANCEL_NOTIFICATION_HANDLE,
                NotificationId::SignalId(1),
            )]),
            next_notification_handle: NotificationHandle(17),
        }
    }

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
            restate.journal.completion.id = ?completion_id,
        ),
        ret
    )]
    pub(crate) fn cache_run_completion(
        &mut self,
        completion_id: CompletionId,
        notification_result: NotificationResult,
    ) {
        self.cached_run_completions
            .insert(completion_id, notification_result);
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.journal.completion.id = ?completion_id,
        ),
        ret
    )]
    pub(crate) fn enqueue_run_completion_ack(
        &mut self,
        completion_id: CompletionId,
    ) -> Result<(), Error> {
        if let Some(res) = self.cached_run_completions.remove(&completion_id) {
            self.to_process.push_back(Notification {
                id: NotificationId::CompletionId(completion_id),
                result: res,
            });
            Ok(())
        } else {
            Err(BadProposeRunCompletionAck::new(completion_id).into())
        }
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
        self.push_ready(notification.id, notification.result);
    }

    fn push_ready(&mut self, id: NotificationId, result: NotificationResult) {
        // Completion ids are delivered once, so only signals can have more than one delivery queued.
        let queue = self.signal_delivery == SignalDelivery::Queued
            && matches!(
                id,
                NotificationId::SignalId(_) | NotificationId::SignalName(_)
            );
        match self.ready.entry(id) {
            Entry::Occupied(mut e) if queue => e.get_mut().push_back(result),
            Entry::Occupied(mut e) => *e.get_mut() = VecDeque::from([result]),
            Entry::Vacant(e) => {
                e.insert(VecDeque::from([result]));
            }
        }
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
        // * ALL_COMPLETED -> Wait for every child to complete, regardless of success or failure. Completes always with success (same as JS Promise.allSettled).
        // * FIRST_SUCCEEDED_OR_ALL_FAILED -> Resolve on the first success; fail only if all children fail (same as JS Promise.any).
        // * ALL_SUCCEEDED_OR_FIRST_FAILED -> Resolve when all children succeed; short-circuit on the first failure (same as JS Promise.all).
        // * UNKNOWN -> Unknown combinator made of 1 or more children futures. Only the SDK can determine its state (success/failure/pending).
        //              The SDK uses this for futures that are not representable in any of the above types.
        //              A notable example if RestatePromise.map in Javascript or Java SDK.
        //
        // Implementation note: for performance reasons, the nested combinators use Vec to store children nodes, but they're conceptually sets.
        // Duplicates should not influence the wake-up algorithm.
        //
        // # SDK and restate-server expectations
        // The SDK will try to resolve the nodes of the tree it can resolve.
        //
        // If the SDK can fully resolve the future with the local information, it will unblock the user code and move on
        // If the SDK cannot resolve the future, it will shave off the tree the Completed nodes, and propagate back the remaining part of the future tree.
        //      Propagation will happen depending on the situation, via AwaitingOnMessage or SuspensionMessage.
        //
        // # About this function
        // try_resolve_future loops:
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
        //
        // # Cost
        // Reducing the whole tree after every popped notification costs O(n) per notification for
        // a tree with n leaves, so O(n^2) for an await whose n notifications are all queued, as
        // happens on replay. So the reduction of the whole tree is only done for the first two
        // rounds (no notification popped, then the first one popped), which covers awaits that
        // need at most one notification at the cost they always had. From the second popped
        // notification on, the remaining tree is indexed (see IndexedFuture), and each popped
        // notification only visits the leaves it completes and the ancestors that change with
        // them, for the same result: the same ResolveFutureResult, after popping the same
        // notifications, with the same remaining tree.
        //
        // The index relies on this invariant: once _try_resolve_future returned Ok(Pending), every
        // leaf left in the tree is pending (Single returns its own state; a combinator either
        // removes a completed child, or short-circuits with Err, or completes itself). Popping a
        // notification changes `ready` only for its id, so the only leaves that can change state
        // are those whose handle maps to that id, and they all take the state of that
        // notification (the id was not in `ready` before, or those leaves would not be pending).
        // Reducing the tree again would return Pending without changing anything in the subtrees
        // without such a leaf, so those can be skipped.

        let mut full_reductions = 0;
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

                    // If there's some element in the notification queue, we can pop it and try to resolve the future again.
                    // Otherwise, the only possible way to make progress now is either reading from input stream, or running a ctx.run if any.
                    if self.to_process.is_empty() {
                        return ResolveFutureResult::WaitExternalInput(unresolved_future);
                    }
                    full_reductions += 1;
                    if full_reductions == 2 {
                        break;
                    }
                    self.pop_notification_queue();
                }
            }
        }

        // The tree was reduced to Pending and there are more notifications to pop:
        // continue on the index.
        let mut indexed_future = IndexedFuture::new(unresolved_future, &self.handle_mapping);
        while let Some(Notification { id, result }) = self.to_process.pop_front() {
            let state = if result.is_failure() {
                HandleState::Failed
            } else {
                HandleState::Succeeded
            };
            let leaves = indexed_future.leaves.remove(&id);
            // Pending leaves map to ids not in `ready` (see above), so `result` becomes the
            // first delivery of `id` and gives these leaves their state.
            debug_assert!(leaves.is_none() || !self.ready.contains_key(&id));
            self.push_ready(id, result);
            if let Some(leaves) = leaves {
                if indexed_future.complete_leaves(leaves, state) {
                    return ResolveFutureResult::AnyCompleted;
                }
            }
        }
        ResolveFutureResult::WaitExternalInput(indexed_future.into_unresolved_future())
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
            self.push_ready(notif.id, notif.result);
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
            .is_some_and(|id| self.ready.contains_key(id))
    }

    fn resolve_handle_state(&self, handle: NotificationHandle) -> HandleState {
        match self
            .handle_mapping
            .get(&handle)
            .and_then(|id| self.ready.get(id))
            .and_then(VecDeque::front)
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
        let deliveries = self.ready.get_mut(id)?;
        let res = deliveries.pop_front();
        if deliveries.is_empty() {
            self.ready.remove(id);
        }
        if handle != CANCEL_NOTIFICATION_HANDLE {
            self.handle_mapping.remove(&handle);
            // Don't remove the CANCEL handle mapping
            // TODO improve the code here around handling built-in signals handles
        }
        res
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
        self.ready
            .get(self.handle_mapping.get(&handle)?)?
            .front()
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

/// An [`UnresolvedFuture`] whose leaves are all pending, as a tree with parent links and leaves
/// indexed by notification id, so that a notification is applied in time proportional to the
/// leaves it completes and the combinators that complete with them, instead of the whole tree.
///
/// [`IndexedFuture::complete_leaves`] does to the tree what [`AsyncResultsState::_try_resolve_future`]
/// does when those leaves are the only ones that are not pending: it tells whether the future
/// can be resolved (that one returns `Err`, or a completed state) and otherwise removes the same
/// children in the same order (`swap_remove`), so that the remaining tree is the same.
///
/// A change to the combinator rules of `_try_resolve_future` must be made here too: the
/// differential test in this file runs the two side by side and fails until they agree.
struct IndexedFuture {
    // nodes[0] is the root. Nodes removed from the tree stay here, unreachable.
    nodes: Vec<Node>,
    // The pending leaves, by the notification id their handle maps to. Leaves whose handle has
    // no mapping never complete, and are not here.
    leaves: HashMap<NotificationId, Vec<u32>>,
    // Scratch space of complete_leaves: (depth of the parent, parent, completed child) for each
    // completed node, so that the deepest parents come first.
    completed: BinaryHeap<(u32, u32, u32)>,
    // Scratch space of complete_leaves: the positions of the completed children of a node.
    positions: Vec<usize>,
}

struct Node {
    kind: NodeKind,
    parent: u32,
    // The index of this node in the parent's children.
    position: u32,
    depth: u32,
    // Pending until the node completes, then how it completed.
    state: HandleState,
    children: Vec<u32>,
}

#[derive(Clone, Copy)]
enum NodeKind {
    Single(NotificationHandle),
    FirstCompleted,
    AllCompleted,
    FirstSucceededOrAllFailed,
    AllSucceededOrFirstFailed,
    Unknown,
}

const NO_PARENT: u32 = u32::MAX;

impl IndexedFuture {
    fn new(
        unresolved_future: UnresolvedFuture,
        handle_mapping: &HashMap<NotificationHandle, NotificationId>,
    ) -> Self {
        let mut indexed_future = IndexedFuture {
            nodes: Vec::new(),
            leaves: HashMap::new(),
            completed: BinaryHeap::new(),
            positions: Vec::new(),
        };
        indexed_future.push(unresolved_future, NO_PARENT, 0, 0, handle_mapping);
        indexed_future
    }

    fn push(
        &mut self,
        unresolved_future: UnresolvedFuture,
        parent: u32,
        position: u32,
        depth: u32,
        handle_mapping: &HashMap<NotificationHandle, NotificationId>,
    ) -> u32 {
        let index = u32::try_from(self.nodes.len()).expect("future tree too large");
        let (kind, children) = match unresolved_future {
            UnresolvedFuture::Single(handle) => {
                if let Some(id) = handle_mapping.get(&handle) {
                    self.leaves.entry(id.clone()).or_default().push(index);
                }
                (NodeKind::Single(handle), vec![])
            }
            UnresolvedFuture::FirstCompleted(children) => (NodeKind::FirstCompleted, children),
            UnresolvedFuture::AllCompleted(children) => (NodeKind::AllCompleted, children),
            UnresolvedFuture::FirstSucceededOrAllFailed(children) => {
                (NodeKind::FirstSucceededOrAllFailed, children)
            }
            UnresolvedFuture::AllSucceededOrFirstFailed(children) => {
                (NodeKind::AllSucceededOrFirstFailed, children)
            }
            UnresolvedFuture::Unknown(children) => (NodeKind::Unknown, children),
        };
        self.nodes.push(Node {
            kind,
            parent,
            position,
            depth,
            state: HandleState::Pending,
            children: Vec::with_capacity(children.len()),
        });
        for (child_position, child) in children.into_iter().enumerate() {
            let child = self.push(
                child,
                index,
                child_position as u32,
                depth + 1,
                handle_mapping,
            );
            self.nodes[index as usize].children.push(child);
        }
        index
    }

    /// Completes these leaves, all pending until now, with `state`. Returns true when the future
    /// can be resolved, after which this tree must not be used anymore.
    fn complete_leaves(&mut self, leaves: Vec<u32>, state: HandleState) -> bool {
        self.completed.clear();
        for leaf in leaves {
            if self.complete_node(leaf, state) {
                return true;
            }
        }
        // A node's children are one level deeper than the node, so all of them are done when
        // its first completed child comes out of the heap.
        while let Some((_, parent, child)) = self.completed.pop() {
            let parent_kind = self.nodes[parent as usize].kind;
            self.positions.clear();
            let mut next = Some(child);
            while let Some(child) = next {
                let child = &self.nodes[child as usize];
                match (parent_kind, child.state) {
                    // The combinators that short-circuit in _try_resolve_future.
                    (NodeKind::FirstCompleted | NodeKind::Unknown, _)
                    | (NodeKind::FirstSucceededOrAllFailed, HandleState::Succeeded)
                    | (NodeKind::AllSucceededOrFirstFailed, HandleState::Failed) => return true,
                    _ => {}
                }
                self.positions.push(child.position as usize);
                next = match self.completed.peek() {
                    Some(&(_, p, c)) if p == parent => {
                        self.completed.pop();
                        Some(c)
                    }
                    _ => None,
                };
            }
            self.remove_completed_children(parent);

            if self.nodes[parent as usize].children.is_empty() {
                let parent_state = match parent_kind {
                    NodeKind::AllCompleted | NodeKind::AllSucceededOrFirstFailed => {
                        HandleState::Succeeded
                    }
                    NodeKind::FirstSucceededOrAllFailed => HandleState::Failed,
                    NodeKind::Single(_) | NodeKind::FirstCompleted | NodeKind::Unknown => {
                        unreachable!("only combinators have children, and these short-circuit")
                    }
                };
                if self.complete_node(parent, parent_state) {
                    return true;
                }
            }
        }
        false
    }

    // Marks the node completed and queues its parent for complete_leaves. Returns true if the
    // node is the root, that is when the future can be resolved.
    fn complete_node(&mut self, node: u32, state: HandleState) -> bool {
        let node_ref = &mut self.nodes[node as usize];
        node_ref.state = state;
        let parent = node_ref.parent;
        if parent == NO_PARENT {
            return true;
        }
        self.completed
            .push((self.nodes[parent as usize].depth, parent, node));
        false
    }

    // Removes the children of `parent` at `self.positions`, which are completed, as the loops of
    // _try_resolve_future do: going through the children in order, a completed child is replaced
    // by the last one (swap_remove), which is checked in turn. Children that are not completed do
    // not move unless they are last, so only the completed ones need a look.
    fn remove_completed_children(&mut self, parent: u32) {
        self.positions.sort_unstable();
        let mut children = std::mem::take(&mut self.nodes[parent as usize].children);
        for &position in &self.positions {
            // Past the end: the child was last when an earlier one was removed, so it was moved
            // there, and removed in turn, being completed.
            while position < children.len() {
                children.swap_remove(position);
                let Some(&moved) = children.get(position) else {
                    break;
                };
                let moved = &mut self.nodes[moved as usize];
                moved.position = position as u32;
                if !moved.state.is_completed() {
                    break;
                }
            }
        }
        self.nodes[parent as usize].children = children;
    }

    fn into_unresolved_future(mut self) -> UnresolvedFuture {
        self.take_unresolved_future(0)
    }

    fn take_unresolved_future(&mut self, node: u32) -> UnresolvedFuture {
        let kind = self.nodes[node as usize].kind;
        let children = std::mem::take(&mut self.nodes[node as usize].children)
            .into_iter()
            .map(|child| self.take_unresolved_future(child))
            .collect();
        match kind {
            NodeKind::Single(handle) => UnresolvedFuture::Single(handle),
            NodeKind::FirstCompleted => UnresolvedFuture::FirstCompleted(children),
            NodeKind::AllCompleted => UnresolvedFuture::AllCompleted(children),
            NodeKind::FirstSucceededOrAllFailed => {
                UnresolvedFuture::FirstSucceededOrAllFailed(children)
            }
            NodeKind::AllSucceededOrFirstFailed => {
                UnresolvedFuture::AllSucceededOrFirstFailed(children)
            }
            NodeKind::Unknown => UnresolvedFuture::Unknown(children),
        }
    }
}

impl UnresolvedFuture {
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
    use bytes::Bytes;
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
        => WaitExternalInput(first_completed!(unknown!(all_completed!(unknown!(2))))));
    test_try_future_resolve!(deep_unknown_resolves_when_all_done:
        state([success(1), success(2)]),
        first_completed!(unknown!(all_completed!(1, unknown!(2))))
        => AnyCompleted);

    // ==================== AllCompleted ====================

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

    test_try_future_resolve!(fsaf_asff_unknown_success:
        state([success(3)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => AnyCompleted);
    test_try_future_resolve!(fsaf_asff_unknown_failure:
        state([failure(3)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => AnyCompleted);
    // 1 fails → fsaf prunes it, fsaf(2) still pending. 3 still pending.
    test_try_future_resolve!(nested_fsaf_asff_partial:
        state([failure(1)]),
        first_succeeded_or_all_failed!(1, all_succeeded_or_first_failed!(2, unknown!(3)))
        => WaitExternalInput(first_succeeded_or_all_failed!(all_succeeded_or_first_failed!(2, unknown!(3)))));
    // fsaf(1, all_completed(2, unknown(3))) — deep extraction
    test_try_future_resolve!(fsaf_with_nested_unknown_in_all_completed:
        state([success(3)]),
        first_succeeded_or_all_failed!(1, all_completed!(2, unknown!(3)))
        => AnyCompleted);
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
        => WaitExternalInput(first_succeeded_or_all_failed!(1, unknown!(all_succeeded_or_first_failed!(3)))));

    // ==================== AllSucceededOrFirstFailed ====================

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
    test_try_future_resolve!(asff_with_unknown_shortcircuits:
        state([failure(2)]),
        all_succeeded_or_first_failed!(1, unknown!(2))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_leaf:
        state([success(1)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => WaitExternalInput(all_succeeded_or_first_failed!(unknown!(2, first_succeeded_or_all_failed!(3, 4)))));
    test_try_future_resolve!(asff_unknown_fsaf_resolves_on_inner_fsaf_success:
        state([success(3)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => AnyCompleted);
    // 3 fails but 4 pending → fsaf pending → nothing AnyCompleted
    test_try_future_resolve!(asff_unknown_fsaf_failure_doesnt_resolve:
        state([failure(3)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => WaitExternalInput(all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(4)))));
    // Both 3 and 4 fail → fsaf AnyCompleted → unknown wakes
    test_try_future_resolve!(asff_unknown_fsaf_all_inner_fail:
        state([failure(3), failure(4)]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_fsaf_pending:
        state([]),
        all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))
        => WaitExternalInput(all_succeeded_or_first_failed!(1, unknown!(2, first_succeeded_or_all_failed!(3, 4)))));

    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_partial_1:
        state([success(1)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => WaitExternalInput(all_succeeded_or_first_failed!(unknown!(all_completed!(2, unknown!(3))))));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_partial_2:
        state([success(2)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => WaitExternalInput(all_succeeded_or_first_failed!(1, unknown!(all_completed!(unknown!(3))))));
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_shortcircuits_failure:
        state([failure(1)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => AnyCompleted);
    test_try_future_resolve!(asff_unknown_all_completed_with_unknown_all_done:
        state([success(2), success(3)]),
        all_succeeded_or_first_failed!(1, unknown!(all_completed!(2, unknown!(3))))
        => AnyCompleted);

    test_try_future_resolve!(asff_with_nested_first_completed:
        state([failure(2)]),
        all_succeeded_or_first_failed!(first_completed!(1, 2), first_completed!(3, 4))
        => AnyCompleted);

    test_try_future_resolve!(asff_with_nested_all_completed:
        state([failure(1), failure(2)]),
        all_succeeded_or_first_failed!(all_completed!(1, 2), all_completed!(3, 4))
         => WaitExternalInput(all_succeeded_or_first_failed!(all_completed!(3, 4))));
    test_try_future_resolve!(asff_with_nested_all_completed_only_one_resolved:
        state([failure(1)]),
        all_succeeded_or_first_failed!(all_completed!(1, 2), all_completed!(3, 4))
         => WaitExternalInput(all_succeeded_or_first_failed!(all_completed!(2), all_completed!(3, 4))));

    // ==================== Unknown ====================

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
            combinator_type: eq(CombinatorType::Unknown as i32)
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
                combinator_type: eq(CombinatorType::Unknown as i32)
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
                combinator_type: eq(CombinatorType::Unknown as i32)
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
            combinator_type: eq(CombinatorType::Unknown as i32)
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
                combinator_type: eq(CombinatorType::Unknown as i32)
            })],
            combinator_type: eq(CombinatorType::AllCompleted as i32)
        })
    );

    // --- Repeated deliveries of the same id ---

    fn signal_delivery_state(
        signal_delivery: SignalDelivery,
        id: NotificationId,
        values: &[&'static [u8]],
    ) -> (AsyncResultsState, NotificationHandle) {
        let mut state = AsyncResultsState::new(signal_delivery);
        let handle = state.create_handle_mapping(id.clone());
        for v in values {
            state.enqueue(Notification {
                id: id.clone(),
                result: NotificationResult::Value(Bytes::from_static(v).into()),
            });
        }
        // Drain to_process into ready
        while state.pop_notification_queue() {}
        (state, handle)
    }

    fn value(v: &'static [u8]) -> NotificationResult {
        NotificationResult::Value(Bytes::from_static(v).into())
    }

    #[test]
    fn queued_signal_deliveries_are_taken_in_order() {
        let (mut state, h1) = signal_delivery_state(
            SignalDelivery::Queued,
            NotificationId::SignalName("a".to_owned()),
            &[b"1", b"2"],
        );
        assert_eq!(state.copy_handle(h1), Some(value(b"1")));
        assert_eq!(state.take_handle(h1), Some(value(b"1")));

        let h2 = state.create_handle_mapping(NotificationId::SignalName("a".to_owned()));
        assert!(state.is_handle_completed(h2));
        assert_eq!(state.take_handle(h2), Some(value(b"2")));

        let h3 = state.create_handle_mapping(NotificationId::SignalName("a".to_owned()));
        assert!(!state.is_handle_completed(h3));
        assert!(state.ready.is_empty());
    }

    #[test]
    fn overwrite_signal_deliveries_keep_the_last() {
        let (mut state, h1) = signal_delivery_state(
            SignalDelivery::Overwrite,
            NotificationId::SignalName("a".to_owned()),
            &[b"1", b"2"],
        );
        assert_eq!(state.take_handle(h1), Some(value(b"2")));

        let h2 = state.create_handle_mapping(NotificationId::SignalName("a".to_owned()));
        assert!(!state.is_handle_completed(h2));
    }

    #[test]
    fn queued_does_not_apply_to_completion_ids() {
        let (mut state, h1) = signal_delivery_state(
            SignalDelivery::Queued,
            NotificationId::CompletionId(1),
            &[b"1", b"2"],
        );
        assert_eq!(state.take_handle(h1), Some(value(b"2")));
        assert!(state.ready.is_empty());
    }

    // ==================== Indexed resolution (third notification on) ====================

    // The first two notifications go through the full reduction, the next ones through the index.
    test_try_future_resolve!(indexed_swap_remove_order:
        state([success(1), success(2), success(5), success(3)]),
        all_completed!(1, 2, 3, 4, 5, 6)
        => WaitExternalInput(all_completed!(6, 4)));
    // The completed duplicates of 1 at the end are moved to the front and removed in turn.
    test_try_future_resolve!(indexed_duplicates_moved_from_the_tail:
        state([success(7), success(8), success(1)]),
        all_completed!(1, 2, 3, 1, 1)
        => WaitExternalInput(all_completed!(3, 2)));
    // The failed fsaf is removed from all_completed, which moves 4 in its place.
    test_try_future_resolve!(indexed_nested_completion_propagates:
        state([success(7), success(8), failure(1), failure(2), success(4)]),
        all_completed!(first_succeeded_or_all_failed!(1, 2), 3, 4)
        => WaitExternalInput(all_completed!(3)));
    test_try_future_resolve!(indexed_nested_completion_short_circuits:
        state([success(7), success(8), failure(1), failure(2), success(4)]),
        all_succeeded_or_first_failed!(first_succeeded_or_all_failed!(1, 2), 3, 4)
        => AnyCompleted);
    test_try_future_resolve!(indexed_short_circuit:
        state([success(7), success(8), success(4), failure(2), success(3)]),
        all_succeeded_or_first_failed!(first_completed!(1, 2), 3, 4)
        => AnyCompleted);
    test_try_future_resolve!(indexed_unrelated_notifications_skipped:
        state([success(7), success(8), success(9), success(10)]),
        all_completed!(1, 2)
        => WaitExternalInput(all_completed!(1, 2)));

    #[test]
    fn indexed_stops_at_the_resolving_notification() {
        let mut state = state([success(1), success(2), success(3), success(4), success(5)]);
        assert_eq!(
            state.try_resolve_future(all_completed!(1, 2, 3)),
            AnyCompleted
        );
        // 4 and 5 stay queued for the next await.
        assert_eq!(state.to_process.len(), 2);
        assert!((1..=3).all(|h| state.is_handle_completed(h.into())));
        assert!(!state.is_handle_completed(4.into()));
    }

    // ==================== Differential test against the full reduction ====================

    impl AsyncResultsState {
        /// try_resolve_future without the index: the whole tree reduced again after every
        /// popped notification. The reference of the differential test.
        fn try_resolve_future_reference(
            &mut self,
            mut unresolved_future: UnresolvedFuture,
        ) -> ResolveFutureResult {
            loop {
                match self._try_resolve_future(&mut unresolved_future) {
                    Ok(handle_state) if handle_state.is_completed() => return AnyCompleted,
                    Err(_) => return AnyCompleted,
                    Ok(_) => {
                        if !self.pop_notification_queue() {
                            return WaitExternalInput(unresolved_future);
                        }
                    }
                }
            }
        }

        fn queued(&self) -> Vec<(NotificationId, NotificationResult)> {
            self.to_process
                .iter()
                .map(|n| (n.id.clone(), n.result.clone()))
                .collect()
        }
    }

    mod differential {
        use super::*;
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        // Ids the handles map to, several handles sharing some of them.
        fn tree_id(rng: &mut StdRng) -> NotificationId {
            match rng.gen_range(0..10) {
                0..=5 => NotificationId::CompletionId(rng.gen_range(1..=6)),
                6..=7 => NotificationId::SignalId(rng.gen_range(17..=18)),
                _ => NotificationId::SignalName(["a", "b"][rng.gen_range(0..2)].to_owned()),
            }
        }

        fn notification_id(rng: &mut StdRng) -> NotificationId {
            match rng.gen_range(0..12) {
                0..=8 => tree_id(rng),
                // The cancel signal, which handle 1 maps to.
                9 => NotificationId::SignalId(1),
                // Ids no handle maps to.
                10 => NotificationId::CompletionId(rng.gen_range(100..=102)),
                _ => NotificationId::SignalName("unrelated".to_owned()),
            }
        }

        fn notification(
            rng: &mut StdRng,
            counter: &mut u32,
        ) -> (NotificationId, NotificationResult) {
            *counter += 1;
            let result = if rng.gen_bool(0.3) {
                NotificationResult::Failure(Failure {
                    code: 500,
                    message: counter.to_string(),
                    ..Default::default()
                })
            } else {
                NotificationResult::Value(Bytes::from(counter.to_string()).into())
            };
            (notification_id(rng), result)
        }

        // Handles 20..36 are mapped, 36..40 are not, 1 is the cancel signal.
        fn handle(rng: &mut StdRng) -> NotificationHandle {
            if rng.gen_bool(0.05) {
                CANCEL_NOTIFICATION_HANDLE
            } else {
                rng.gen_range(20..40).into()
            }
        }

        fn tree(rng: &mut StdRng, depth: u32) -> UnresolvedFuture {
            if depth == 0 || rng.gen_bool(0.3) {
                return UnresolvedFuture::Single(handle(rng));
            }
            let len = if rng.gen_bool(0.05) {
                0
            } else {
                rng.gen_range(1..=6)
            };
            let children = (0..len).map(|_| tree(rng, depth - 1)).collect();
            match rng.gen_range(0..5) {
                0 => UnresolvedFuture::FirstCompleted(children),
                1 => UnresolvedFuture::AllCompleted(children),
                2 => UnresolvedFuture::FirstSucceededOrAllFailed(children),
                3 => UnresolvedFuture::AllSucceededOrFirstFailed(children),
                _ => UnresolvedFuture::Unknown(children),
            }
        }

        // Returns how many awaits popped more than two notifications, which the index handles.
        fn run(seed: u64) -> usize {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut counter = 0;
            let signal_delivery = if rng.gen_bool(0.5) {
                SignalDelivery::Queued
            } else {
                SignalDelivery::Overwrite
            };
            let mut new = AsyncResultsState::new(signal_delivery);
            let mut reference = AsyncResultsState::new(signal_delivery);
            for handle in 20..36 {
                let id = tree_id(&mut rng);
                new.handle_mapping.insert(handle.into(), id.clone());
                reference.handle_mapping.insert(handle.into(), id);
            }
            for _ in 0..rng.gen_range(0..3) {
                let (id, result) = notification(&mut rng, &mut counter);
                new.insert_ready(Notification {
                    id: id.clone(),
                    result: result.clone(),
                });
                reference.insert_ready(Notification { id, result });
            }

            let depth = rng.gen_range(1..=4);
            let mut indexed_awaits = 0;
            let mut future = tree(&mut rng, depth);
            for step in 0..20 {
                for _ in 0..rng.gen_range(0..16) {
                    let (id, result) = notification(&mut rng, &mut counter);
                    new.enqueue(Notification {
                        id: id.clone(),
                        result: result.clone(),
                    });
                    reference.enqueue(Notification { id, result });
                }

                let queued = reference.to_process.len();
                let new_result = new.try_resolve_future(future.clone());
                let reference_result = reference.try_resolve_future_reference(future.clone());
                let context = || format!("seed {seed}, step {step}, future {future:?}");
                assert_eq!(new_result, reference_result, "{}", context());
                assert_eq!(new.ready, reference.ready, "{}", context());
                assert_eq!(new.queued(), reference.queued(), "{}", context());
                if queued - reference.to_process.len() > 2 {
                    indexed_awaits += 1;
                }

                match new_result {
                    WaitExternalInput(remaining) => future = remaining,
                    AnyCompleted => {
                        // The SDK takes some of the completed handles, then awaits again.
                        for handle in future.handles() {
                            let completed = new.is_handle_completed(handle);
                            assert_eq!(completed, reference.is_handle_completed(handle));
                            if completed && rng.gen_bool(0.7) {
                                assert_eq!(new.take_handle(handle), reference.take_handle(handle));
                            }
                        }
                        if rng.gen_bool(0.5) {
                            future = tree(&mut rng, depth);
                        }
                    }
                }
            }
            indexed_awaits
        }

        #[test]
        fn indexed_resolution_matches_full_reduction() {
            // More with e.g. `DIFFERENTIAL_CASES=1000000 cargo test --release --lib differential`.
            let cases = std::env::var("DIFFERENTIAL_CASES")
                .map(|cases| cases.parse().expect("DIFFERENTIAL_CASES"))
                .unwrap_or(1_000);
            let indexed_awaits: usize = (0..cases).map(run).sum();
            println!("{cases} cases, {indexed_awaits} awaits that used the index");
            // Make sure the index was exercised.
            assert!(indexed_awaits > cases as usize, "{indexed_awaits}");
        }
    }
}
