//! The [`ExplicitRecoveryVM`] implementation, shaped as [`crate::CoreVM`]: a state machine whose
//! states hold the attempt's state, moved by transitions ([`transitions`]) through
//! [`CoreExplicitRecoveryVM::do_transition`].

mod async_results_state;
mod context;
pub(crate) mod errors;
mod local_state;
mod span_state;
mod transitions;

pub use errors::missing_features;

use super::{
    ActionOptions, AttemptState, ExplicitRecoveryVM, Input, Notification, Options, ResponseHead,
    SpanId, StateScope,
};
use crate::service_protocol::explicit_recovery::messages::atomic_group_action_message;
use crate::service_protocol::explicit_recovery::{Decoder, ProtocolVersion};
use crate::vm::errors::{
    codes, empty_idempotency_key, empty_limit_key, empty_scope, missing_content_type, ClosedError,
    OutOfBoundsDuration, UnexpectedStateError,
};
use crate::{
    AttachInvocationTarget, CallHandle, Error, Header, HeaderMap, NonEmptyValue,
    NotificationHandle, SendHandle, Target, VMResult,
};
use async_results_state::AsyncResultsState;
use bytes::{Buf, Bytes};
use context::{Context, Output};
use local_state::{LocalState, StateOp};
use span_state::{SpanOp, SpanState};
use std::borrow::Cow;
use std::fmt;
use std::time::Duration;
use strum::IntoStaticStr;
use transitions::*;

const CONTENT_TYPE: &str = "content-type";

/// What a running attempt holds.
#[derive(Debug)]
pub(crate) struct Attempt {
    async_results: AsyncResultsState,
    local_state: LocalState,
    spans: SpanState,
}

/// The open atomic group: its actions, kept until the group is closed, and what they change
/// in the attempt once it is.
#[derive(Debug, Default)]
pub(crate) struct OpenGroup {
    actions: Vec<atomic_group_action_message::Action>,
    state_ops: Vec<StateOp>,
    span_ops: Vec<SpanOp>,
    /// The handles the actions in the group returned.
    handles: Vec<NotificationHandle>,
}

#[derive(Debug, IntoStaticStr)]
pub(crate) enum State {
    WaitingStart,
    /// The handler runs.
    Running(Attempt),
    /// The handler runs, and opened an atomic group.
    InAtomicGroup(Attempt, OpenGroup),
    /// The handler ended: `EndMessage` was written.
    Closed,
}

impl State {
    fn as_unexpected_state(&self, event: impl ToString) -> Error {
        match self {
            State::Closed => ClosedError::new(event.to_string()).into(),
            State::InAtomicGroup(..) => errors::not_allowed_in_atomic_group(event.to_string()),
            s => UnexpectedStateError::new(s.into(), event.to_string()).into(),
        }
    }
}

/// The [`ExplicitRecoveryVM`] implementation.
pub struct CoreExplicitRecoveryVM {
    // Input decoder
    decoder: Decoder,

    // State machine
    context: Context,
    /// `Err`: the attempt failed (`ErrorMessage` was written), or was shut down
    /// (`ShutdownMessage` was).
    last_transition: Result<State, Error>,
}

// --- Bound checks
#[allow(unused)]
const fn is_send<T: Send>() {}
const _: () = is_send::<CoreExplicitRecoveryVM>();

impl fmt::Debug for CoreExplicitRecoveryVM {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("CoreExplicitRecoveryVM");
        s.field("version", &self.context.negotiated_protocol_version);
        if let Some(start_info) = self.context.start_info() {
            s.field("invocation_id", &start_info.debug_id);
            s.field("attempt", &start_info.attempt);
        }
        match &self.last_transition {
            Ok(state) => s.field("last_transition", &<&'static str>::from(state)),
            Err(_) => s.field("last_transition", &"Errored"),
        };
        s.finish()
    }
}

impl CoreExplicitRecoveryVM {
    fn verify_target(&mut self, target: &Target) -> VMResult<()> {
        if target.idempotency_key.as_deref() == Some("") {
            self.do_transition(HitError(empty_idempotency_key()))?;
        }
        if target.scope.as_deref() == Some("") {
            self.do_transition(HitError(empty_scope()))?;
        }
        if target.limit_key.as_deref() == Some("") {
            self.do_transition(HitError(empty_limit_key()))?;
        }
        if target.limit_key.is_some() && target.scope.is_none() {
            self.do_transition(HitError(errors::limit_key_without_scope()))?;
        }
        Ok(())
    }

    fn invoke_time(&mut self, invoke_at: Option<Duration>) -> VMResult<u64> {
        match u64::try_from(invoke_at.unwrap_or_default().as_millis()) {
            Ok(invoke_time) => Ok(invoke_time),
            Err(e) => {
                self.do_transition(HitError(OutOfBoundsDuration("invoke time", e).into()))?;
                unreachable!();
            }
        }
    }
}

impl ExplicitRecoveryVM for CoreExplicitRecoveryVM {
    fn new(request_headers: impl HeaderMap, options: Options) -> VMResult<Self> {
        let version = request_headers
            .extract(CONTENT_TYPE)
            .map_err(|e| {
                Error::new(
                    codes::BAD_REQUEST,
                    format!("cannot read '{CONTENT_TYPE}' header: {e:?}"),
                )
            })?
            .ok_or_else(missing_content_type)?
            .parse::<ProtocolVersion>()?;

        Ok(Self {
            decoder: Decoder::for_registry(),
            context: Context {
                start_info: None,
                negotiated_protocol_version: version,
                options,
                ids: Default::default(),
                input_is_closed: false,
                output: Output::new(),
                cancel_received: false,
                cancel_taken: false,
            },
            last_transition: Ok(State::WaitingStart),
        })
    }

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
                    // A message type this version does not define: the runtime violated the
                    // protocol.
                    let _ = self.do_transition(HitError(errors::protocol_violation(e.to_string())));
                    return;
                }
            }
        }
    }

    fn notify_input_closed(&mut self) {
        self.context.input_is_closed = true;
        let _ = self.do_transition(NotifyInputClosed);
    }

    fn take_output(&mut self) -> Bytes {
        self.context
            .output
            .buffer
            .copy_to_bytes(self.context.output.buffer.remaining())
    }

    fn should_close_output(&self) -> bool {
        self.context.output.is_closed() && self.context.input_is_closed
    }

    fn is_ready_to_execute(&self) -> VMResult<bool> {
        match &self.last_transition {
            Ok(State::WaitingStart) => Ok(false),
            Ok(State::Running(_)) | Ok(State::InAtomicGroup(..)) => Ok(true),
            Ok(s) => Err(s.as_unexpected_state("IsReadyToExecute")),
            Err(e) => Err(e.clone()),
        }
    }

    fn get_input(&mut self) -> VMResult<Input> {
        self.do_transition(GetInput)
    }

    fn is_feature_supported(&self, feature: u32) -> bool {
        // Version 1 has no feature in its baseline.
        self.context
            .start_info()
            .is_some_and(|start_info| start_info.features.contains(&feature))
    }

    fn take_next_notification(&mut self) -> VMResult<Option<Notification>> {
        self.do_transition(TakeNextNotification)
    }

    fn cmd_state_get(
        &mut self,
        scope: StateScope,
        key: String,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateGet(scope, key, options))
    }

    fn cmd_state_get_keys(
        &mut self,
        scope: StateScope,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateGetKeys(scope, options))
    }

    fn cmd_state_set(
        &mut self,
        scope: StateScope,
        key: String,
        value: Bytes,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(CmdStateSet(scope, key, value, options, want_result))
    }

    fn cmd_state_clear(
        &mut self,
        scope: StateScope,
        key: String,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(CmdStateClear(scope, key, options, want_result))
    }

    fn cmd_state_clear_all(
        &mut self,
        scope: StateScope,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(CmdStateClearAll(scope, options, want_result))
    }

    fn cmd_call(
        &mut self,
        target: Target,
        input: Bytes,
        invoke_at: Option<Duration>,
        options: ActionOptions,
    ) -> VMResult<CallHandle> {
        self.verify_target(&target)?;
        let invoke_time = self.invoke_time(invoke_at)?;
        let (invocation_id_notification_handle, result) =
            self.do_transition(CmdCall(target, input, invoke_time, options, true))?;
        Ok(CallHandle {
            invocation_id_notification_handle,
            call_notification_handle: result.expect("the call asked for its result"),
        })
    }

    fn cmd_send(
        &mut self,
        target: Target,
        input: Bytes,
        invoke_at: Option<Duration>,
        options: ActionOptions,
    ) -> VMResult<SendHandle> {
        self.verify_target(&target)?;
        let invoke_time = self.invoke_time(invoke_at)?;
        let (invocation_id_notification_handle, _) =
            self.do_transition(CmdCall(target, input, invoke_time, options, false))?;
        Ok(SendHandle {
            invocation_id_notification_handle,
        })
    }

    fn cmd_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle> {
        if let AttachInvocationTarget::WorkflowId {
            scope: Some(scope), ..
        }
        | AttachInvocationTarget::IdempotencyId {
            scope: Some(scope), ..
        } = &target
        {
            if scope.is_empty() {
                self.do_transition(HitError(empty_scope()))?;
            }
        }
        self.do_transition(CmdAttachInvocation(target, options))
    }

    fn cmd_cancel_invocation(
        &mut self,
        invocation_id: String,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(CmdCancelInvocation(invocation_id, options, want_result))
    }

    fn cmd_signal_read(
        &mut self,
        name: String,
        index: u32,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdSignalRead(name, index, options))
    }

    fn cmd_signal_send(
        &mut self,
        target_invocation_id: String,
        name: String,
        value: Option<NonEmptyValue>,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(CmdSignalSend(
            target_invocation_id,
            name,
            value,
            options,
            want_result,
        ))
    }

    fn cmd_flush(&mut self, options: ActionOptions) -> VMResult<NotificationHandle> {
        self.do_transition(CmdFlush(options))
    }

    fn atomic_group_start(&mut self) -> VMResult<()> {
        self.do_transition(AtomicGroupStart)
    }

    fn atomic_group_end(
        &mut self,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>> {
        self.do_transition(AtomicGroupEnd(options, want_result))
    }

    fn atomic_group_abort(&mut self) -> VMResult<Vec<NotificationHandle>> {
        self.do_transition(AtomicGroupAbort)
    }

    fn span_start(&mut self, name: String, parent: Option<SpanId>) -> VMResult<SpanId> {
        self.do_transition(SpanStart(name, parent))
    }

    fn span_end(&mut self, span: SpanId, result: Option<NonEmptyValue>) -> VMResult<()> {
        self.do_transition(SpanEnd(span, result))
    }

    fn heartbeat(&mut self) -> VMResult<()> {
        self.do_transition(Heartbeat)
    }

    fn end(&mut self, output: NonEmptyValue) -> VMResult<()> {
        self.do_transition(CmdEnd(output))
    }

    fn notify_error(&mut self, error: Error) {
        let _ = self.do_transition(HitError(error));
    }

    fn shutdown(&mut self) {
        if matches!(
            self.last_transition,
            Ok(State::Running(_)) | Ok(State::InAtomicGroup(..))
        ) {
            let _ = self.do_transition(HitShutdown);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.context.cancel_taken
            || self
                .context
                .start_info()
                .is_some_and(|start_info| start_info.cancelled)
    }

    fn state(&self) -> AttemptState {
        match &self.last_transition {
            Ok(State::WaitingStart) => AttemptState::Starting,
            Ok(State::Running(_)) | Ok(State::InAtomicGroup(..)) => AttemptState::Running,
            Ok(State::Closed) | Err(_) => AttemptState::Closed,
        }
    }
}
