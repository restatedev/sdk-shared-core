use crate::headers::HeaderMap;
use crate::service_protocol::messages::{
    attach_invocation_command_message, complete_awakeable_command_message,
    complete_promise_command_message, get_invocation_output_command_message,
    output_command_message, send_signal_command_message, AttachInvocationCommandMessage,
    CallCommandMessage, CompleteAwakeableCommandMessage, CompletePromiseCommandMessage,
    ErrorBehavior, GetInvocationOutputCommandMessage, GetPromiseCommandMessage,
    IdempotentRequestTarget, OneWayCallCommandMessage, OutputCommandMessage,
    PeekPromiseCommandMessage, SendSignalCommandMessage, SleepCommandMessage, WorkflowTarget,
};
use crate::service_protocol::{
    CompletionId, Decoder, Notification, NotificationId, NotificationResult, RawMessage, Version,
    CANCEL_SIGNAL_ID,
};
use crate::vm::errors::{
    ClosedError, OutOfBoundsDuration, UnexpectedStateError, UnsupportedFeatureForNegotiatedVersion,
    EMPTY_IDEMPOTENCY_KEY, EMPTY_LIMIT_KEY, EMPTY_SCOPE, SUSPENDED,
};
use crate::vm::run_state::RunState;
use crate::vm::transitions::*;
use crate::{
    AttachInvocationTarget, AwaitResponse, AwakeableHandle, CallHandle, CommandRelationship, Error,
    Header, ImplicitCancellationOption, Input, JournalMode, NonDeterministicChecksOption,
    NonEmptyValue, NotificationHandle, PayloadOptions, ResponseHead, Restore, RetryPolicy,
    RunExitResult, RunHandle, SendHandle, StepBegin, Target, TerminalFailure, TxBegin,
    UnresolvedFuture, VMOptions, VMResult, Value, CANCEL_NOTIFICATION_HANDLE,
};
use async_results_state::AsyncResultsState;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{alphabet, Engine};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use context::{Context, EagerState, Output};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::time::Duration;
use std::{fmt, mem};
use storage::{EntryKind, RestoreFromJournal, StorageJournal};
use strum::IntoStaticStr;
use tracing::{debug, enabled, instrument, Level};
use tx::TxState;
// Needed by the inherent helpers calling the VM methods
use crate::VM as _;

// Macro used for informative debug logs
macro_rules! invocation_debug_logs {
    ($this:expr, $($arg:tt)*) => {
        if ($this.is_processing()) {
            tracing::debug!($($arg)*)
        }
    };
}

mod async_results_state;
mod context;
pub(crate) mod errors;
mod run_state;
mod storage;
mod transitions;
pub(crate) mod tx;

const CONTENT_TYPE: &str = "content-type";

#[derive(Debug, IntoStaticStr)]
pub(crate) enum State {
    WaitingStart,
    WaitingReplayEntries {
        received_entries: u32,
        commands: VecDeque<RawMessage>,
        async_results: AsyncResultsState,
        eager_state: EagerState,
    },
    Replaying {
        commands: VecDeque<RawMessage>,
        run_state: RunState,
        async_results: AsyncResultsState,
        eager_state: EagerState,
    },
    Processing {
        processing_first_entry: bool,
        run_state: RunState,
        async_results: AsyncResultsState,
        eager_state: EagerState,
    },
    Closed,
}

impl State {
    fn as_unexpected_state(&self, event: impl ToString) -> Error {
        if matches!(self, State::Closed) {
            return ClosedError::new(event.to_string()).into();
        }
        UnexpectedStateError::new(self.into(), event.to_string()).into()
    }

    /// Tries to transition to Processing when the condition is met, in all the other cases returns the current state.
    #[inline]
    fn try_transition_to_processing(self) -> Self {
        match self {
            State::Replaying {
                commands,
                async_results,
                eager_state,
                run_state,
            } if commands.is_empty() => State::Processing {
                processing_first_entry: true,
                run_state,
                async_results,
                eager_state,
            },
            s => s,
        }
    }

    #[inline]
    fn eager_state(&self) -> Option<&EagerState> {
        match self {
            State::WaitingReplayEntries { eager_state, .. }
            | State::Replaying { eager_state, .. }
            | State::Processing { eager_state, .. } => Some(eager_state),
            _ => None,
        }
    }

    #[inline]
    fn eager_state_mut(&mut self) -> Option<&mut EagerState> {
        match self {
            State::WaitingReplayEntries { eager_state, .. }
            | State::Replaying { eager_state, .. }
            | State::Processing { eager_state, .. } => Some(eager_state),
            _ => None,
        }
    }
}

struct TrackedInvocationId {
    handle: NotificationHandle,
    invocation_id: Option<String>,
}

impl TrackedInvocationId {
    fn is_resolved(&self) -> bool {
        self.invocation_id.is_some()
    }
}

pub struct CoreVM {
    options: VMOptions,

    // Input decoder
    decoder: Decoder,

    // State machine
    context: Context,
    last_transition: Result<State, Error>,

    // Implicit cancellation tracking
    tracked_invocation_ids: Vec<TrackedInvocationId>,

    // Run names, useful for debugging
    sys_run_names: HashMap<NotificationHandle, String>,

    // Transactional handler state
    tx: TxState,

    // Index of the journal, in JournalMode::Storage, set by sys_restore
    storage: Option<StorageJournal>,
}

impl CoreVM {
    // Returns empty string if the invocation id is not present
    fn debug_invocation_id(&self) -> &str {
        if let Some(start_info) = self.context.start_info() {
            &start_info.debug_id
        } else {
            ""
        }
    }

    fn debug_state(&self) -> &'static str {
        match &self.last_transition {
            Ok(s) => s.into(),
            Err(_) => "Failed",
        }
    }

    fn verify_error_metadata_feature_support(&mut self, value: &NonEmptyValue) -> VMResult<()> {
        if let NonEmptyValue::Failure(f) = value {
            if !f.metadata.is_empty() {
                self.verify_feature_support("terminal error metadata", Version::V6)?;
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn verify_feature_support(
        &mut self,
        feature: &'static str,
        minimum_required_protocol: Version,
    ) -> VMResult<()> {
        if self.context.negotiated_protocol_version < minimum_required_protocol {
            return self.do_transition(HitError(
                UnsupportedFeatureForNegotiatedVersion::new(
                    feature,
                    self.context.negotiated_protocol_version,
                    minimum_required_protocol,
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn tx_read<T>(
        &mut self,
        op: &'static str,
        f: impl FnOnce(Option<&tx::WriteSet>, &EagerState) -> Result<T, tx::PartialStateError>,
    ) -> VMResult<T> {
        let res = match &self.last_transition {
            Err(e) => return Err(e.clone()),
            Ok(state) => match (state.eager_state(), &self.tx) {
                (Some(snapshot), TxState::Inactive) => f(None, snapshot),
                (Some(snapshot), TxState::Executing { write_set, .. }) => {
                    f(Some(write_set), snapshot)
                }
                (Some(_), tx) => {
                    let e = tx_unexpected_state(op, tx);
                    self.do_transition(HitError(e))?;
                    unreachable!();
                }
                (None, _) => {
                    let e = state.as_unexpected_state(op);
                    self.do_transition(HitError(e))?;
                    unreachable!();
                }
            },
        };
        match res {
            Ok(t) => Ok(t),
            Err(tx::PartialStateError) => {
                self.do_transition(HitError(errors::TX_PARTIAL_STATE))?;
                unreachable!();
            }
        }
    }

    fn tx_write(
        &mut self,
        op: &'static str,
        f: impl FnOnce(&mut tx::WriteSet, &mut Vec<OneWayCallCommandMessage>),
    ) -> VMResult<()> {
        if let Err(e) = &self.last_transition {
            return Err(e.clone());
        }
        let e = match &mut self.tx {
            TxState::Executing {
                write_set, sends, ..
            } => {
                f(write_set, sends);
                return Ok(());
            }
            tx => tx_unexpected_state(op, tx),
        };
        self.do_transition(HitError(e))?;
        unreachable!();
    }

    fn emit_apply_commands(
        &mut self,
        commands: impl IntoIterator<Item = storage::ApplyCommand>,
    ) -> VMResult<()> {
        for command in commands {
            match command {
                storage::ApplyCommand::ClearAllState => self.sys_state_clear_all()?,
                storage::ApplyCommand::SetState(key, value) => {
                    self.sys_state_set(key, value, PayloadOptions::default())?
                }
                storage::ApplyCommand::ClearState(key) => self.sys_state_clear(key)?,
                storage::ApplyCommand::Send(mut send) => {
                    invocation_debug_logs!(
                        self,
                        "Executing 'Send to {}/{}'",
                        send.service_name,
                        send.handler_name
                    );
                    let completion_id = self.context.journal.next_completion_notification_id();
                    send.invocation_id_notification_idx = completion_id;
                    self.do_transition(SysSimpleCompletableEntry(
                        send,
                        completion_id,
                        PayloadOptions::default(),
                    ))?;
                }
            }
        }
        Ok(())
    }

    fn is_storage_mode(&self) -> bool {
        self.options.journal_mode == JournalMode::Storage
    }

    /// Returns true if the VM is in storage mode, failing if the journal was not restored yet.
    fn verify_storage_restored(&mut self) -> VMResult<bool> {
        if !self.is_storage_mode() {
            return Ok(false);
        }
        if self.storage.is_none() {
            self.do_transition(HitError(Error::new(
                errors::codes::INTERNAL,
                "In storage journal mode, sys_restore must be called right after sys_input. This is an SDK bug.",
            )))?;
            unreachable!();
        }
        Ok(true)
    }

    /// Returns the value of `key` in the eager state, if known.
    fn eager_state_get(&self, key: &str) -> Option<Option<Bytes>> {
        let snapshot = self.last_transition.as_ref().ok()?.eager_state()?;
        tx::snapshot_get(key, snapshot).ok()
    }

    /// Returns the state keys from the eager state, if known.
    fn eager_state_keys(&self) -> Option<Vec<String>> {
        let snapshot = self.last_transition.as_ref().ok()?.eager_state()?;
        tx::snapshot_keys(snapshot).ok()
    }

    /// Creates an handle already completed with `result`, without writing anything to the journal.
    fn storage_ready_handle(&mut self, result: NotificationResult) -> VMResult<NotificationHandle> {
        let completion_id = self.context.journal.next_completion_notification_id();
        match &mut self.last_transition {
            Ok(State::Processing { async_results, .. }) => {
                let id = NotificationId::CompletionId(completion_id);
                let handle = async_results.create_handle_mapping(id.clone());
                async_results.insert_ready(Notification { id, result });
                Ok(handle)
            }
            Err(e) => Err(e.clone()),
            Ok(s) => {
                let e = s.as_unexpected_state("storage read");
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        }
    }

    /// Creates handles for completion ids of commands already in the journal.
    fn storage_map_handles(
        &mut self,
        completion_ids: &[CompletionId],
    ) -> VMResult<Vec<NotificationHandle>> {
        match &mut self.last_transition {
            Ok(State::Processing { async_results, .. }) => Ok(completion_ids
                .iter()
                .map(|id| async_results.create_handle_mapping(NotificationId::CompletionId(*id)))
                .collect()),
            Err(e) => Err(e.clone()),
            Ok(s) => {
                let e = s.as_unexpected_state("storage lookup");
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        }
    }

    fn track_call_invocation_id(&mut self, handle: NotificationHandle) {
        if matches!(
            self.options.implicit_cancellation,
            ImplicitCancellationOption::Enabled {
                cancel_children_calls: true,
                ..
            }
        ) {
            self.tracked_invocation_ids.push(TrackedInvocationId {
                handle,
                invocation_id: None,
            })
        }
    }

    /// Reads the commit record of a completed step, without taking the notification.
    fn peek_committed_record(
        &self,
        completion_id: CompletionId,
    ) -> Result<tx::TxCommitRecord, Error> {
        let result = match &self.last_transition {
            Ok(State::Processing { async_results, .. }) => {
                async_results.get_ready(&NotificationId::CompletionId(completion_id))
            }
            _ => None,
        };
        match result {
            Some(NotificationResult::Value(v)) => tx::decode_commit_record(v.content.clone()),
            _ => Err(Error::new(
                errors::codes::PROTOCOL_VIOLATION,
                format!("Missing or unexpected step commit record for completion {completion_id}"),
            )),
        }
    }

    /// Takes the commit record from the completed commit run notification.
    /// The outer error is the VM error, the inner one is a record decoding error.
    fn take_committed_record(
        &mut self,
        handle: NotificationHandle,
    ) -> VMResult<Result<tx::TxCommitRecord, Error>> {
        Ok(match super::VM::take_notification(self, handle)? {
            Some(Value::Success(b)) => tx::decode_commit_record(b),
            Some(v) => Err(Error::new(
                errors::codes::PROTOCOL_VIOLATION,
                format!(
                    "Unexpected transaction commit result variant {}",
                    <&'static str>::from(v)
                ),
            )),
            None => Err(Error::new(
                errors::codes::INTERNAL,
                "sys_tx_end was called before the transaction commit was durable",
            )),
        })
    }

    fn verify_target(&mut self, target: &Target) -> VMResult<()> {
        if let Some(idempotency_key) = &target.idempotency_key {
            if idempotency_key.is_empty() {
                self.do_transition(HitError(EMPTY_IDEMPOTENCY_KEY))?;
                unreachable!();
            }
        }
        if let Some(scope) = &target.scope {
            if scope.is_empty() {
                self.do_transition(HitError(EMPTY_SCOPE))?;
                unreachable!();
            }
        }
        if let Some(limit_key) = &target.limit_key {
            if limit_key.is_empty() {
                self.do_transition(HitError(EMPTY_LIMIT_KEY))?;
                unreachable!();
            }
        }
        if target.scope.is_some() {
            self.verify_feature_support("scope", Version::V7)?;
        }
        if target.limit_key.is_some() {
            self.verify_feature_support("limit key", Version::V7)?;
        }
        Ok(())
    }

    fn _is_completed(&self, handle: NotificationHandle) -> bool {
        match &self.last_transition {
            Ok(State::Replaying { async_results, .. })
            | Ok(State::Processing { async_results, .. }) => {
                async_results.is_handle_completed(handle)
            }
            _ => false,
        }
    }

    fn _do_progress(
        &mut self,
        unresolved_future: UnresolvedFuture,
    ) -> Result<AwaitResponse, Error> {
        match self.do_transition(DoProgress(unresolved_future)) {
            Ok(Ok(do_progress_response)) => Ok(do_progress_response),
            Ok(Err(_)) => Err(SUSPENDED),
            Err(e) => Err(e),
        }
    }

    fn is_implicit_cancellation_enabled(&self) -> bool {
        matches!(
            self.options.implicit_cancellation,
            ImplicitCancellationOption::Enabled { .. }
        )
    }

    fn is_processing(&self) -> bool {
        matches!(&self.last_transition, Ok(State::Processing { .. }))
    }
}

impl fmt::Debug for CoreVM {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("CoreVM");
        s.field("version", &self.context.negotiated_protocol_version);

        if let Some(start_info) = self.context.start_info() {
            s.field("invocation_id", &start_info.debug_id);
        }

        match &self.last_transition {
            Ok(state) => s.field("last_transition", &<&'static str>::from(state)),
            Err(_) => s.field("last_transition", &"Errored"),
        };

        s.field("command_index", &self.context.journal.command_index())
            .field(
                "notification_index",
                &self.context.journal.notification_index(),
            )
            .finish()
    }
}

// --- Bound checks
#[allow(unused)]
const fn is_send<T: Send>() {}
const _: () = is_send::<CoreVM>();

impl super::VM for CoreVM {
    #[instrument(level = "trace", skip(request_headers), ret)]
    fn new(request_headers: impl HeaderMap, options: VMOptions) -> Result<Self, Error> {
        let version = request_headers
            .extract(CONTENT_TYPE)
            .map_err(|e| {
                Error::new(
                    errors::codes::BAD_REQUEST,
                    format!("cannot read '{CONTENT_TYPE}' header: {e:?}"),
                )
            })?
            .ok_or(errors::MISSING_CONTENT_TYPE)?
            .parse::<Version>()?;

        if version < Version::minimum_supported_version()
            || version > Version::maximum_supported_version()
        {
            return Err(Error::new(
                errors::codes::UNSUPPORTED_MEDIA_TYPE,
                format!(
                    "Unsupported protocol version {:?}, not within [{:?} to {:?}]. \
                    You might need to rediscover the service, check https://docs.restate.dev/references/errors/#RT0015",
                    version,
                    Version::minimum_supported_version(),
                    Version::maximum_supported_version()
                ),
            ));
        }
        let non_deterministic_checks_ignore_payload_equality = matches!(
            options.non_determinism_checks,
            NonDeterministicChecksOption::PayloadChecksDisabled
        );
        let awaiting_on_policy = options.awaiting_on_policy;

        Ok(Self {
            options,
            decoder: Decoder::new(version),
            context: Context {
                input_is_closed: false,
                output: Output::new(version),
                start_info: None,
                journal: Default::default(),
                non_deterministic_checks_ignore_payload_equality,
                negotiated_protocol_version: version,
                awaiting_on_policy,
            },
            last_transition: Ok(State::WaitingStart),
            tracked_invocation_ids: vec![],
            sys_run_names: HashMap::with_capacity(0),
            tx: TxState::default(),
            storage: None,
        })
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn get_response_head(&self) -> ResponseHead {
        ResponseHead {
            status_code: 200,
            headers: vec![Header {
                key: Cow::Borrowed(CONTENT_TYPE),
                value: Cow::Borrowed(self.context.negotiated_protocol_version.content_type()),
            }],
            version: self.context.negotiated_protocol_version,
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn notify_input(&mut self, buffer: Bytes) {
        self.decoder.push(buffer);
        loop {
            match self.decoder.consume_next() {
                Ok(Some(msg)) => {
                    if self.do_transition(NewMessage(msg)).is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    return;
                }
                Err(e) => {
                    if self.do_transition(HitError(e.into())).is_err() {
                        return;
                    }
                }
            }
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn notify_input_closed(&mut self) {
        self.context.input_is_closed = true;
        let _ = self.do_transition(NotifyInputClosed);
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn notify_error(
        &mut self,
        mut error: Error,
        command_relationship: Option<CommandRelationship>,
    ) {
        if error.behavior != ErrorBehavior::Retry
            && self
                .verify_feature_support("error behavior", Version::V7)
                .is_err()
        {
            return;
        }

        if let Some(command_relationship) = command_relationship {
            error = error.with_related_command_metadata(
                self.context
                    .journal
                    .resolve_related_command(command_relationship),
            );
        }

        let _ = self.do_transition(HitError(error));
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn take_output(&mut self) -> Bytes {
        self.context
            .output
            .buffer
            .copy_to_bytes(self.context.output.buffer.remaining())
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn is_ready_to_execute(&self) -> Result<bool, Error> {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingReplayEntries { .. }) => Ok(false),
            Ok(State::Processing { .. }) | Ok(State::Replaying { .. }) => Ok(true),
            Ok(s) => Err(s.as_unexpected_state("IsReadyToExecute")),
            Err(e) => Err(e.clone()),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn is_completed(&self, handle: NotificationHandle) -> bool {
        self._is_completed(handle)
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn do_await(&mut self, unresolved_future: UnresolvedFuture) -> VMResult<AwaitResponse> {
        // Once a transaction started, the invocation is not cancellable anymore:
        // the only thing left to await is the commit record durability.
        if self.is_implicit_cancellation_enabled() && matches!(self.tx, TxState::Inactive) {
            // We want the runtime to wake us up in case cancel notification comes in.
            let unresolved_future_with_cancellation = UnresolvedFuture::FirstCompleted(vec![
                unresolved_future,
                UnresolvedFuture::Single(CANCEL_NOTIFICATION_HANDLE),
            ]);

            match self._do_progress(unresolved_future_with_cancellation) {
                Ok(AwaitResponse::AnyCompleted) => {
                    // If it's cancel signal, then let's go on with the cancellation logic
                    if self._is_completed(CANCEL_NOTIFICATION_HANDLE) {
                        // Loop once over the tracked invocation ids to resolve the unresolved ones
                        for i in 0..self.tracked_invocation_ids.len() {
                            if self.tracked_invocation_ids[i].is_resolved() {
                                continue;
                            }

                            let handle = self.tracked_invocation_ids[i].handle;

                            // Try to resolve it
                            match self._do_progress(UnresolvedFuture::Single(handle)) {
                                Ok(AwaitResponse::AnyCompleted) => {
                                    let invocation_id = match self.do_transition(CopyNotification(handle)) {
                                        Ok(Ok(Some(Value::InvocationId(invocation_id)))) => Ok(invocation_id),
                                        Ok(Err(_)) => Err(SUSPENDED),
                                        _ => panic!("Unexpected variant! If the id handle is completed, it must be an invocation id handle!")
                                    }?;

                                    // This handle is resolved
                                    self.tracked_invocation_ids[i].invocation_id =
                                        Some(invocation_id);
                                }
                                res => return res,
                            }
                        }

                        // Now we got all the invocation IDs, let's cancel!
                        for tracked_invocation_id in mem::take(&mut self.tracked_invocation_ids) {
                            self.sys_cancel_invocation(
                                tracked_invocation_id
                                    .invocation_id
                                    .expect("We resolved before all the invocation ids"),
                            )?;
                        }

                        // Flip the cancellation
                        let _ = self.take_notification(CANCEL_NOTIFICATION_HANDLE);

                        // Done
                        Ok(AwaitResponse::CancelSignalReceived)
                    } else {
                        Ok(AwaitResponse::AnyCompleted)
                    }
                }
                res => res,
            }
        } else {
            self._do_progress(unresolved_future)
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn take_notification(&mut self, handle: NotificationHandle) -> VMResult<Option<Value>> {
        match self.do_transition(TakeNotification(handle)) {
            Ok(Ok(Some(value))) => {
                if self.is_implicit_cancellation_enabled() {
                    // Let's check if that's one of the tracked invocation ids
                    // We can do binary search here because we assume tracked_invocation_ids is ordered, as handles are incremental numbers
                    if let Ok(found) = self
                        .tracked_invocation_ids
                        .binary_search_by(|tracked| tracked.handle.cmp(&handle))
                    {
                        let Value::InvocationId(invocation_id) = &value else {
                            panic!("Expecting an invocation id here, but got {value:?}");
                        };
                        // Keep track of this invocation id
                        self.tracked_invocation_ids
                            .get_mut(found)
                            .unwrap()
                            .invocation_id = Some(invocation_id.clone());
                    }
                }

                Ok(Some(value))
            }
            Ok(Ok(None)) => Ok(None),
            Ok(Err(_)) => Err(SUSPENDED),
            Err(e) => Err(e),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_input(&mut self) -> Result<Input, Error> {
        self.do_transition(SysInput)
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_state_get(
        &mut self,
        key: String,
        options: PayloadOptions,
    ) -> Result<NotificationHandle, Error> {
        invocation_debug_logs!(self, "Executing 'Get state {key}'");
        if self.verify_storage_restored()? {
            // State reads are not journaled, unless they need to fetch the state from the runtime
            if let Some(value) = self.eager_state_get(&key) {
                return self.storage_ready_handle(match value {
                    Some(v) => NotificationResult::Value(v.into()),
                    None => NotificationResult::Void(Default::default()),
                });
            }
        }
        self.do_transition(SysStateGet(key, options))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_state_get_keys(&mut self) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Get state keys'");
        if self.verify_storage_restored()? {
            if let Some(keys) = self.eager_state_keys() {
                return self.storage_ready_handle(NotificationResult::StateKeys(
                    crate::service_protocol::messages::StateKeys {
                        keys: keys.into_iter().map(Bytes::from).collect(),
                    },
                ));
            }
        }
        self.do_transition(SysStateGetKeys)
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_state_set(
        &mut self,
        key: String,
        value: Bytes,
        options: PayloadOptions,
    ) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Set state {key}'");
        self.do_transition(SysStateSet(key, value, options))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_state_clear(&mut self, key: String) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear state {key}'");
        self.do_transition(SysStateClear(key))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_state_clear_all(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear all state'");
        self.do_transition(SysStateClearAll)
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_sleep(
        &mut self,
        name: String,
        wake_up_time_since_unix_epoch: Duration,
        now_since_unix_epoch: Option<Duration>,
    ) -> VMResult<NotificationHandle> {
        if self.is_processing() {
            match (&name, now_since_unix_epoch) {
                (name, Some(now_since_unix_epoch)) if name.is_empty() => {
                    debug!(
                        "Executing 'Timer with duration {:?}'",
                        wake_up_time_since_unix_epoch.saturating_sub(now_since_unix_epoch)
                    );
                }
                (name, Some(now_since_unix_epoch)) => {
                    debug!(
                        "Executing 'Timer {name} with duration {:?}'",
                        wake_up_time_since_unix_epoch.saturating_sub(now_since_unix_epoch)
                    );
                }
                (name, None) if name.is_empty() => {
                    debug!("Executing 'Timer'");
                }
                (name, None) => {
                    debug!("Executing 'Timer named {name}'");
                }
            }
        }
        let wake_up_time = match u64::try_from(wake_up_time_since_unix_epoch.as_millis()) {
            Ok(d) => d,
            Err(e) => {
                self.do_transition(HitError(OutOfBoundsDuration("sleep duration", e).into()))?;
                unreachable!();
            }
        };
        let mut name = name;
        if self.verify_storage_restored()? {
            let storage = self.storage.as_mut().expect("storage mode is restored");
            let key = storage.next_key(
                EntryKind::Sleep,
                if name.is_empty() { "sleep" } else { &name },
            );
            if let Some(completion_id) = storage.sleeps.get(&key).copied() {
                return Ok(self.storage_map_handles(&[completion_id])?[0]);
            }
            name = key;
        }
        let completion_id = self.context.journal.next_completion_notification_id();

        self.do_transition(SysSimpleCompletableEntry(
            SleepCommandMessage {
                wake_up_time,
                result_completion_id: completion_id,
                name,
            },
            completion_id,
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_call(
        &mut self,
        target: Target,
        input: Bytes,
        name: Option<String>,
        options: PayloadOptions,
    ) -> VMResult<CallHandle> {
        invocation_debug_logs!(
            self,
            "Executing 'Call {}/{}'",
            target.service,
            target.handler
        );
        self.verify_target(&target)?;

        let mut name = name;
        if self.verify_storage_restored()? {
            let storage = self.storage.as_mut().expect("storage mode is restored");
            let key = storage.next_key(
                EntryKind::Call,
                &name
                    .take()
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| format!("{}/{}", target.service, target.handler)),
            );
            if let Some((invocation_id_completion_id, result_completion_id)) =
                storage.calls.get(&key).copied()
            {
                // Already issued by a previous attempt, just await its result
                let handles =
                    self.storage_map_handles(&[invocation_id_completion_id, result_completion_id])?;
                self.track_call_invocation_id(handles[0]);
                return Ok(CallHandle {
                    invocation_id_notification_handle: handles[0],
                    call_notification_handle: handles[1],
                });
            }
            name = Some(key);
        }

        let call_invocation_id_completion_id =
            self.context.journal.next_completion_notification_id();
        let result_completion_id = self.context.journal.next_completion_notification_id();

        let handles = self.do_transition(SysCompletableEntryWithMultipleCompletions(
            CallCommandMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                idempotency_key: target.idempotency_key,
                scope: target.scope,
                limit_key: target.limit_key,
                headers: target
                    .headers
                    .into_iter()
                    .map(crate::service_protocol::messages::Header::from)
                    .collect(),
                parameter: input,
                invocation_id_notification_idx: call_invocation_id_completion_id,
                name: name.unwrap_or_default(),
                result_completion_id,
            },
            vec![call_invocation_id_completion_id, result_completion_id],
            options,
        ))?;

        self.track_call_invocation_id(handles[0]);

        Ok(CallHandle {
            invocation_id_notification_handle: handles[0],
            call_notification_handle: handles[1],
        })
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_send(
        &mut self,
        target: Target,
        input: Bytes,
        delay: Option<Duration>,
        name: Option<String>,
        options: PayloadOptions,
    ) -> VMResult<SendHandle> {
        invocation_debug_logs!(
            self,
            "Executing 'Send to {}/{}'",
            target.service,
            target.handler
        );
        self.verify_target(&target)?;
        let invoke_time = match u64::try_from(delay.unwrap_or_default().as_millis()) {
            Ok(d) => d,
            Err(e) => {
                self.do_transition(HitError(OutOfBoundsDuration("send delay", e).into()))?;
                unreachable!();
            }
        };
        let mut name = name;
        if self.verify_storage_restored()? {
            let storage = self.storage.as_mut().expect("storage mode is restored");
            let key = storage.next_key(
                EntryKind::Send,
                &name
                    .take()
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| format!("{}/{}", target.service, target.handler)),
            );
            if let Some(invocation_id_completion_id) = storage.sends.get(&key).copied() {
                // Already sent by a previous attempt
                return Ok(SendHandle {
                    invocation_id_notification_handle: self
                        .storage_map_handles(&[invocation_id_completion_id])?[0],
                });
            }
            name = Some(key);
        }
        let call_invocation_id_completion_id =
            self.context.journal.next_completion_notification_id();
        let invocation_id_notification_handle = self.do_transition(SysSimpleCompletableEntry(
            OneWayCallCommandMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                idempotency_key: target.idempotency_key,
                scope: target.scope,
                limit_key: target.limit_key,
                headers: target
                    .headers
                    .into_iter()
                    .map(crate::service_protocol::messages::Header::from)
                    .collect(),
                parameter: input,
                invoke_time,
                invocation_id_notification_idx: call_invocation_id_completion_id,
                name: name.unwrap_or_default(),
            },
            call_invocation_id_completion_id,
            options,
        ))?;

        if matches!(
            self.options.implicit_cancellation,
            ImplicitCancellationOption::Enabled {
                cancel_children_one_way_calls: true,
                ..
            }
        ) {
            self.tracked_invocation_ids.push(TrackedInvocationId {
                handle: invocation_id_notification_handle,
                invocation_id: None,
            })
        }

        Ok(SendHandle {
            invocation_id_notification_handle,
        })
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_awakeable(&mut self) -> VMResult<AwakeableHandle> {
        invocation_debug_logs!(self, "Executing 'Create awakeable'");
        if self.is_storage_mode() {
            // Awakeable ids are positional, they can't be found again by a non deterministic handler
            self.do_transition(HitError(errors::unsupported_in_storage_mode(
                "awakeables, use named signals instead",
            )))?;
            unreachable!();
        }

        let signal_id = self.context.journal.next_signal_notification_id();

        let handle = self.do_transition(CreateSignalHandle(
            "awakeable",
            NotificationId::SignalId(signal_id),
        ))?;

        Ok(AwakeableHandle {
            id: awakeable_id_str(&self.context.expect_start_info().id, signal_id),
            handle,
        })
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_complete_awakeable(
        &mut self,
        id: String,
        value: NonEmptyValue,
        options: PayloadOptions,
    ) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Complete awakeable {id}'");
        self.verify_error_metadata_feature_support(&value)?;
        self.do_transition(SysNonCompletableEntry(
            CompleteAwakeableCommandMessage {
                awakeable_id: id,
                result: Some(match value {
                    NonEmptyValue::Success(s) => {
                        complete_awakeable_command_message::Result::Value(s.into())
                    }
                    NonEmptyValue::Failure(f) => {
                        complete_awakeable_command_message::Result::Failure(f.into())
                    }
                }),
                ..Default::default()
            },
            options,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn create_signal_handle(&mut self, signal_name: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Create named signal'");

        self.do_transition(CreateSignalHandle(
            "named awakeable",
            NotificationId::SignalName(signal_name),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_complete_signal(
        &mut self,
        target_invocation_id: String,
        signal_name: String,
        value: NonEmptyValue,
    ) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Complete named signal {signal_name}'");
        self.verify_error_metadata_feature_support(&value)?;
        self.do_transition(SysNonCompletableEntry(
            SendSignalCommandMessage {
                target_invocation_id,
                signal_id: Some(send_signal_command_message::SignalId::Name(signal_name)),
                result: Some(match value {
                    NonEmptyValue::Success(s) => {
                        send_signal_command_message::Result::Value(s.into())
                    }
                    NonEmptyValue::Failure(f) => {
                        send_signal_command_message::Result::Failure(f.into())
                    }
                }),
                ..Default::default()
            },
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_get_promise(&mut self, key: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Await promise {key}'");

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            GetPromiseCommandMessage {
                key,
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_peek_promise(&mut self, key: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Peek promise {key}'");

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            PeekPromiseCommandMessage {
                key,
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
        options: PayloadOptions,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Complete promise {key}'");
        self.verify_error_metadata_feature_support(&value)?;

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            CompletePromiseCommandMessage {
                key,
                completion: Some(match value {
                    NonEmptyValue::Success(s) => {
                        complete_promise_command_message::Completion::CompletionValue(s.into())
                    }
                    NonEmptyValue::Failure(f) => {
                        complete_promise_command_message::Completion::CompletionFailure(f.into())
                    }
                }),
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
            options,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_run(&mut self, name: String) -> VMResult<RunHandle> {
        let mut name = name;
        if self.verify_storage_restored()? {
            let storage = self.storage.as_mut().expect("storage mode is restored");
            let key = storage.next_key(EntryKind::Run, if name.is_empty() { "run" } else { &name });
            if let Some(completion_id) = storage.completed_runs.get(&key).copied() {
                return Ok(RunHandle {
                    replayed: true,
                    handle: self.storage_map_handles(&[completion_id])?[0],
                });
            }
            name = key;
        }
        match self.do_transition(SysRun(name.clone())) {
            Ok(handle) => {
                if enabled!(Level::DEBUG) && !handle.replayed {
                    // Store the name, we need it later when completing
                    self.sys_run_names.insert(handle.handle, name);
                }
                Ok(handle)
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(
        level = "trace",
        skip(self, value, retry_policy),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn propose_run_completion(
        &mut self,
        notification_handle: NotificationHandle,
        value: RunExitResult,
        retry_policy: RetryPolicy,
    ) -> VMResult<()> {
        if enabled!(Level::DEBUG) {
            let name = self
                .sys_run_names
                .remove(&notification_handle)
                .unwrap_or_default();
            match &value {
                RunExitResult::Success(_) => {
                    invocation_debug_logs!(self, "Journaling run '{name}' success result");
                }
                RunExitResult::TerminalFailure(TerminalFailure { code, .. }) => {
                    invocation_debug_logs!(
                        self,
                        "Journaling run '{name}' terminal failure {code} result"
                    );
                }
                RunExitResult::RetryableFailure { .. } => {
                    invocation_debug_logs!(self, "Propagating run '{name}' retryable failure");
                }
            }
        }
        if let RunExitResult::TerminalFailure(f) = &value {
            if !f.metadata.is_empty() {
                self.verify_feature_support("terminal error metadata", Version::V6)?;
            }
        }
        if retry_policy.should_pause_on_max_attempts() {
            self.verify_feature_support("pause", Version::V7)?;
        }

        self.do_transition(ProposeRunCompletion(
            notification_handle,
            value,
            retry_policy,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_cancel_invocation(&mut self, target_invocation_id: String) -> VMResult<()> {
        invocation_debug_logs!(
            self,
            "Executing 'Cancel invocation' of {target_invocation_id}"
        );
        self.do_transition(SysNonCompletableEntry(
            SendSignalCommandMessage {
                target_invocation_id,
                signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
                result: Some(send_signal_command_message::Result::Void(Default::default())),
                ..Default::default()
            },
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Attach invocation'");

        match &target {
            AttachInvocationTarget::WorkflowId {
                scope: Some(scope), ..
            }
            | AttachInvocationTarget::IdempotencyId {
                scope: Some(scope), ..
            } => {
                if scope.is_empty() {
                    self.do_transition(HitError(EMPTY_SCOPE))?;
                    unreachable!();
                }
                self.verify_feature_support("scope", Version::V7)?;
            }
            _ => {}
        };

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            AttachInvocationCommandMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        attach_invocation_command_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::WorkflowId { name, key, scope } => {
                        attach_invocation_command_message::Target::WorkflowTarget(WorkflowTarget {
                            workflow_name: name,
                            workflow_key: key,
                            scope,
                        })
                    }
                    AttachInvocationTarget::IdempotencyId {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                        scope,
                    } => attach_invocation_command_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                            scope,
                        },
                    ),
                }),
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_get_invocation_output(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Get invocation output'");

        match &target {
            AttachInvocationTarget::WorkflowId {
                scope: Some(scope), ..
            }
            | AttachInvocationTarget::IdempotencyId {
                scope: Some(scope), ..
            } => {
                if scope.is_empty() {
                    self.do_transition(HitError(EMPTY_SCOPE))?;
                    unreachable!();
                }
                self.verify_feature_support("scope", Version::V7)?;
            }
            _ => {}
        };

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            GetInvocationOutputCommandMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        get_invocation_output_command_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::WorkflowId { name, key, scope } => {
                        get_invocation_output_command_message::Target::WorkflowTarget(
                            WorkflowTarget {
                                workflow_name: name,
                                workflow_key: key,
                                scope,
                            },
                        )
                    }
                    AttachInvocationTarget::IdempotencyId {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                        scope,
                    } => get_invocation_output_command_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                            scope,
                        },
                    ),
                }),
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
            PayloadOptions::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_write_output(&mut self, value: NonEmptyValue, options: PayloadOptions) -> VMResult<()> {
        match &value {
            NonEmptyValue::Success(_) => {
                invocation_debug_logs!(self, "Writing invocation result success value");
            }
            NonEmptyValue::Failure(_) => {
                invocation_debug_logs!(self, "Writing invocation result failure value");
            }
        }
        self.verify_error_metadata_feature_support(&value)?;
        self.do_transition(SysNonCompletableEntry(
            OutputCommandMessage {
                result: Some(match value {
                    NonEmptyValue::Success(b) => output_command_message::Result::Value(b.into()),
                    NonEmptyValue::Failure(f) => output_command_message::Result::Failure(f.into()),
                }),
                ..OutputCommandMessage::default()
            },
            options,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.context.negotiated_protocol_version
        ),
        ret
    )]
    fn sys_end(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "End of the invocation");
        self.do_transition(SysEnd)
    }

    fn sys_tx_begin(&mut self) -> VMResult<TxBegin> {
        if self.is_storage_mode() {
            self.do_transition(HitError(errors::unsupported_in_storage_mode(
                "transactional handlers, use steps instead",
            )))?;
            unreachable!();
        }
        let replaying = match (&self.last_transition, &self.tx) {
            (Err(e), _) => return Err(e.clone()),
            (Ok(State::Replaying { .. }), TxState::Inactive) => Ok(true),
            (Ok(State::Processing { .. }), TxState::Inactive) => Ok(false),
            (Ok(State::Replaying { .. } | State::Processing { .. }), tx) => {
                Err(tx_unexpected_state("tx begin", tx))
            }
            (Ok(s), _) => Err(s.as_unexpected_state("tx begin")),
        };
        let replaying = match replaying {
            Ok(r) => r,
            Err(e) => {
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };

        if replaying {
            // Either the commit record was proposed by a previous attempt, or something else is in the journal,
            // in which case this will fail with a journal mismatch.
            let RunHandle { replayed, handle } = self.sys_run(tx::TX_COMMIT_RUN_NAME.to_owned())?;
            if replayed {
                self.tx = TxState::Committed {
                    handle,
                    proposed_record: None,
                };
                return Ok(TxBegin::Committed(handle));
            }
            self.tx = TxState::Executing {
                commit_run_handle: Some(handle),
                write_set: Default::default(),
                sends: vec![],
                step_key: None,
            };
        } else {
            self.tx = TxState::Executing {
                commit_run_handle: None,
                write_set: Default::default(),
                sends: vec![],
                step_key: None,
            };
        }
        invocation_debug_logs!(self, "Executing transaction");
        Ok(TxBegin::Execute)
    }

    fn tx_state_get(&mut self, key: &str) -> VMResult<Option<Bytes>> {
        self.tx_read("tx get state", |write_set, snapshot| match write_set {
            Some(write_set) => write_set.get(key, snapshot),
            None => tx::snapshot_get(key, snapshot),
        })
    }

    fn tx_state_get_keys(&mut self) -> VMResult<Vec<String>> {
        self.tx_read("tx get state keys", |write_set, snapshot| match write_set {
            Some(write_set) => write_set.keys(snapshot),
            None => tx::snapshot_keys(snapshot),
        })
    }

    fn tx_state_set(&mut self, key: String, value: Bytes) -> VMResult<()> {
        self.tx_write("tx set state", |write_set, _| write_set.set(key, value))
    }

    fn tx_state_clear(&mut self, key: String) -> VMResult<()> {
        self.tx_write("tx clear state", |write_set, _| write_set.clear(key))
    }

    fn tx_state_clear_all(&mut self) -> VMResult<()> {
        self.tx_write("tx clear all state", |write_set, _| write_set.clear_all())
    }

    fn tx_send(
        &mut self,
        target: Target,
        input: Bytes,
        execution_time_since_unix_epoch: Option<Duration>,
        name: Option<String>,
    ) -> VMResult<()> {
        self.verify_target(&target)?;
        let invoke_time = match u64::try_from(
            execution_time_since_unix_epoch
                .unwrap_or_default()
                .as_millis(),
        ) {
            Ok(d) => d,
            Err(e) => {
                self.do_transition(HitError(OutOfBoundsDuration("send delay", e).into()))?;
                unreachable!();
            }
        };
        let send = OneWayCallCommandMessage {
            service_name: target.service,
            handler_name: target.handler,
            key: target.key.unwrap_or_default(),
            idempotency_key: target.idempotency_key,
            scope: target.scope,
            limit_key: target.limit_key,
            headers: target
                .headers
                .into_iter()
                .map(crate::service_protocol::messages::Header::from)
                .collect(),
            parameter: input,
            invoke_time,
            // Assigned when applying the commit record
            invocation_id_notification_idx: 0,
            name: name.unwrap_or_default(),
        };
        self.tx_write("tx send", |_, sends| sends.push(send))
    }

    fn sys_tx_commit(&mut self, output: NonEmptyValue) -> VMResult<NotificationHandle> {
        if let Err(e) = &self.last_transition {
            return Err(e.clone());
        }
        self.verify_error_metadata_feature_support(&output)?;
        let (commit_run_handle, write_set, sends) = match mem::take(&mut self.tx) {
            TxState::Executing {
                commit_run_handle,
                write_set,
                sends,
                step_key: None,
            } => (commit_run_handle, write_set, sends),
            tx => {
                let e = tx_unexpected_state("tx commit", &tx);
                self.tx = tx;
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };

        let record = match output {
            NonEmptyValue::Success(_) => {
                let (clear_all, mutations) = match self
                    .last_transition
                    .as_ref()
                    .ok()
                    .and_then(State::eager_state)
                {
                    Some(snapshot) => write_set.into_mutations(snapshot),
                    None => {
                        let e = self
                            .last_transition
                            .as_ref()
                            .map(|s| s.as_unexpected_state("tx commit"))
                            .unwrap_or_else(|e| e.clone());
                        self.do_transition(HitError(e))?;
                        unreachable!();
                    }
                };
                invocation_debug_logs!(
                    self,
                    "Committing transaction with {} state mutation(s){} and {} one way call(s)",
                    mutations.len(),
                    if clear_all {
                        " after clearing all state"
                    } else {
                        ""
                    },
                    sends.len()
                );
                tx::new_commit_record(clear_all, mutations, sends, output)
            }
            NonEmptyValue::Failure(_) => {
                invocation_debug_logs!(
                    self,
                    "Committing transaction failure, discarding the buffered state mutations and one way calls"
                );
                tx::new_commit_record(false, vec![], vec![], output)
            }
        };

        let handle = match commit_run_handle {
            Some(handle) => handle,
            None => self.sys_run(tx::TX_COMMIT_RUN_NAME.to_owned())?.handle,
        };
        self.propose_run_completion(
            handle,
            RunExitResult::Success(prost::Message::encode_to_vec(&record).into()),
            RetryPolicy::default(),
        )?;
        self.tx = TxState::Committed {
            handle,
            proposed_record: Some(record),
        };
        Ok(handle)
    }

    fn sys_tx_end(&mut self) -> VMResult<NonEmptyValue> {
        let (handle, proposed_record) = match &mut self.tx {
            TxState::Committed {
                handle,
                proposed_record,
            } => (*handle, proposed_record.take()),
            tx => {
                let e = tx_unexpected_state("tx end", tx);
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };

        let record = match proposed_record {
            // Proposed in this attempt: if the SDK didn't wait for the commit to be durable,
            // the commands below are pipelined after the proposal on the same stream.
            Some(record) => Ok(record),
            None => self.take_committed_record(handle)?,
        };
        let (record, output) = match record.and_then(|record| {
            let output = record.output().ok_or_else(|| {
                Error::new(
                    errors::codes::PROTOCOL_VIOLATION,
                    "The transaction commit record has no output",
                )
            })?;
            Ok((record, output))
        }) {
            Ok(r) => r,
            Err(e) => {
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };

        // Apply the record. On replay, this re-emits exactly the same commands,
        // so a partially applied record is completed by the next attempt.
        self.emit_apply_commands(storage::apply_commands(record))?;
        self.sys_write_output(output.clone(), PayloadOptions::default())?;
        self.sys_end()?;
        self.tx = TxState::Ended;
        Ok(output)
    }

    fn sys_restore(&mut self) -> VMResult<Restore> {
        if !self.is_storage_mode() || self.storage.is_some() {
            self.do_transition(HitError(Error::new(
                errors::codes::INTERNAL,
                "sys_restore can be called only once, right after sys_input, in storage journal mode. This is an SDK bug.",
            )))?;
            unreachable!();
        }
        let indexed = self.do_transition(RestoreFromJournal)?;
        self.context
            .journal
            .fast_forward(indexed.commands, indexed.next_completion_id);
        self.storage = Some(indexed.journal);
        invocation_debug_logs!(
            self,
            "Restored invocation from a journal of {} command(s), without replaying it",
            indexed.commands
        );

        // Complete the step the previous attempt was applying, if any
        if let Some((completion_id, applied)) = indexed.incomplete_step {
            // Don't take the notification: the handler will look up the step result
            let record = match self.peek_committed_record(completion_id) {
                Ok(record) => record,
                Err(e) => {
                    self.do_transition(HitError(e))?;
                    unreachable!();
                }
            };
            let missing: Vec<_> = storage::apply_commands(record)
                .into_iter()
                .skip(applied)
                .collect();
            if !missing.is_empty() {
                invocation_debug_logs!(
                    self,
                    "Completing the step applied by the previous attempt: {} command(s) missing",
                    missing.len()
                );
                self.emit_apply_commands(missing)?;
            }
        }

        if indexed.output.is_some() {
            // The previous attempt wrote the output, but didn't end
            invocation_debug_logs!(self, "The invocation output is already in the journal");
            self.sys_end()?;
            return Ok(Restore::Completed);
        }
        Ok(Restore::Execute)
    }

    fn sys_step_begin(&mut self, name: String) -> VMResult<StepBegin> {
        if !self.verify_storage_restored()? {
            self.do_transition(HitError(errors::unsupported_in_storage_mode(
                "steps outside the storage journal mode",
            )))?;
            unreachable!();
        }
        if !matches!(self.tx, TxState::Inactive) {
            let e = tx_unexpected_state("step begin", &self.tx);
            self.do_transition(HitError(e))?;
            unreachable!();
        }
        let storage = self.storage.as_mut().expect("storage mode is restored");
        let key = storage.next_key(EntryKind::Step, &name);
        if let Some(completion_id) = storage.completed_runs.get(&key).copied() {
            invocation_debug_logs!(self, "Step '{key}' already committed");
            return Ok(StepBegin::Committed(
                self.storage_map_handles(&[completion_id])?[0],
            ));
        }
        invocation_debug_logs!(self, "Executing step '{key}'");
        self.tx = TxState::Executing {
            commit_run_handle: None,
            write_set: Default::default(),
            sends: vec![],
            step_key: Some(key),
        };
        Ok(StepBegin::Execute)
    }

    fn sys_step_commit(&mut self, result: NonEmptyValue) -> VMResult<NotificationHandle> {
        if let Err(e) = &self.last_transition {
            return Err(e.clone());
        }
        self.verify_error_metadata_feature_support(&result)?;
        let (key, write_set, sends) = match mem::take(&mut self.tx) {
            TxState::Executing {
                write_set,
                sends,
                step_key: Some(key),
                ..
            } => (key, write_set, sends),
            tx => {
                let e = tx_unexpected_state("step commit", &tx);
                self.tx = tx;
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };

        let record = match result {
            NonEmptyValue::Success(_) => {
                let Some(snapshot) = self
                    .last_transition
                    .as_ref()
                    .ok()
                    .and_then(State::eager_state)
                else {
                    let e = Error::new(
                        errors::codes::INTERNAL,
                        "No state snapshot while committing a step",
                    );
                    self.do_transition(HitError(e))?;
                    unreachable!();
                };
                let (clear_all, mutations) = write_set.into_mutations(snapshot);
                invocation_debug_logs!(
                    self,
                    "Committing step '{key}' with {} state mutation(s){} and {} one way call(s)",
                    mutations.len(),
                    if clear_all {
                        " after clearing all state"
                    } else {
                        ""
                    },
                    sends.len()
                );
                tx::new_commit_record(clear_all, mutations, sends, result)
            }
            NonEmptyValue::Failure(_) => {
                invocation_debug_logs!(
                    self,
                    "Committing step '{key}' failure, discarding the buffered state mutations and one way calls"
                );
                tx::new_commit_record(false, vec![], vec![], result)
            }
        };

        // The commit and its apply commands are written together: if the stream breaks, the runtime keeps a prefix,
        // so the apply commands are never durable without the commit, and only the last step can be partially applied.
        let handle = self.do_transition(SysRun(key))?.handle;
        self.propose_run_completion(
            handle,
            RunExitResult::Success(prost::Message::encode_to_vec(&record).into()),
            RetryPolicy::default(),
        )?;
        self.emit_apply_commands(storage::apply_commands(record))?;
        Ok(handle)
    }

    fn sys_step_take_result(
        &mut self,
        handle: NotificationHandle,
    ) -> VMResult<Option<NonEmptyValue>> {
        if !self.is_completed(handle) {
            return Ok(None);
        }
        let record = match self.take_committed_record(handle)? {
            Ok(record) => record,
            Err(e) => {
                self.do_transition(HitError(e))?;
                unreachable!();
            }
        };
        match record.output() {
            Some(output) => Ok(Some(output)),
            None => {
                self.do_transition(HitError(Error::new(
                    errors::codes::PROTOCOL_VIOLATION,
                    "The step commit record has no result",
                )))?;
                unreachable!();
            }
        }
    }

    #[inline]
    fn state(&self) -> crate::State {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingReplayEntries { .. }) => {
                crate::State::WaitingPreFlight
            }
            Ok(State::Replaying { .. }) => crate::State::Replaying,
            Ok(State::Processing { .. }) => crate::State::Processing,
            Ok(State::Closed) | Err(_) => crate::State::Closed,
        }
    }

    fn last_command_index(&self) -> i64 {
        self.context.journal.command_index()
    }
}

fn tx_unexpected_state(op: &'static str, tx: &TxState) -> Error {
    Error::new(
        errors::codes::INTERNAL,
        format!(
            "Unexpected '{op}' while the transaction is in state {}. This is an SDK bug.",
            tx.name()
        ),
    )
}

const INDIFFERENT_PAD: GeneralPurposeConfig = GeneralPurposeConfig::new()
    .with_decode_padding_mode(DecodePaddingMode::Indifferent)
    .with_encode_padding(false);
const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, INDIFFERENT_PAD);

const AWAKEABLE_PREFIX: &str = "sign_1";

pub(super) fn awakeable_id_str(id: &[u8], completion_index: u32) -> String {
    let mut input_buf = BytesMut::with_capacity(id.len() + size_of::<u32>());
    input_buf.put_slice(id);
    input_buf.put_u32(completion_index);
    format!("{AWAKEABLE_PREFIX}{}", URL_SAFE.encode(input_buf.freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::service_protocol::messages::Future;

    impl CoreVM {
        pub(crate) fn resolve_unresolved_future(
            &self,
            unresolved_future: UnresolvedFuture,
        ) -> Future {
            match &self.last_transition {
                Ok(
                    State::Replaying { async_results, .. }
                    | State::Processing { async_results, .. },
                ) => async_results.resolve_unresolved_future(unresolved_future),
                _ => panic!("Could not resolve unresolved future"),
            }
        }
    }
}
