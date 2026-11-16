//! The [`ExplicitRecoveryVM`] implementation, shaped as [`crate::CoreVM`]'s: a state machine whose
//! states hold the attempt's state, moved by transitions ([`transitions`]) through
//! [`CoreExplicitRecoveryVM::do_transition`].

mod async_results_state;
mod context;
mod earlier_entries;
pub(crate) mod errors;
mod trace_state;
mod transitions;

pub use errors::request_response_unsupported;

use super::{AttemptState, CallHandle, ExplicitRecoveryVM, Notification, SendHandle, TraceId};
use crate::service_protocol::messages::InputCommandMessage;
use crate::service_protocol::{Decoder, Version};
use crate::vm::errors::{
    codes, empty_idempotency_key, empty_limit_key, empty_scope, missing_content_type, ClosedError,
    OutOfBoundsDuration, UnexpectedStateError,
};
use crate::{
    AttachInvocationTarget, Error, Header, HeaderMap, Input, NonEmptyValue, NotificationHandle,
    ResponseHead, Target, VMResult,
};
use async_results_state::AsyncResultsState;
use bytes::{Buf, Bytes};
use context::{Context, EagerState, Output};
use earlier_entries::EarlierEntries;
use std::borrow::Cow;
use std::fmt;
use std::time::Duration;
use strum::IntoStaticStr;
use trace_state::TraceState;
use transitions::*;

const CONTENT_TYPE: &str = "content-type";

#[derive(Debug, IntoStaticStr)]
pub(crate) enum State {
    WaitingStart,
    /// Receiving the entries the earlier attempts stored: nothing is replayed, the VM only takes
    /// from them what [`EarlierEntries`] keeps.
    WaitingEarlierEntries {
        received_entries: u32,
        earlier_entries: EarlierEntries,
        async_results: AsyncResultsState,
        eager_state: EagerState,
    },
    Processing {
        /// The input command, which [`ExplicitRecoveryVM::get_input`] returns.
        input: InputCommandMessage,
        async_results: AsyncResultsState,
        eager_state: EagerState,
        trace_state: TraceState,
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
}

/// The [`ExplicitRecoveryVM`] implementation.
pub struct CoreExplicitRecoveryVM {
    // Input decoder
    decoder: Decoder,

    // State machine
    context: Context,
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
        Ok(())
    }
}

impl ExplicitRecoveryVM for CoreExplicitRecoveryVM {
    fn new(request_headers: impl HeaderMap) -> VMResult<Self> {
        let version = request_headers
            .extract(CONTENT_TYPE)
            .map_err(|e| {
                Error::new(
                    codes::BAD_REQUEST,
                    format!("cannot read '{CONTENT_TYPE}' header: {e:?}"),
                )
            })?
            .ok_or_else(missing_content_type)?
            .parse::<Version>()?;

        if version < Version::V7 || version > Version::maximum_supported_version() {
            return Err(Error::new(
                codes::UNSUPPORTED_MEDIA_TYPE,
                format!(
                    "Unsupported protocol version {version:?} for a session object handler, not within [{:?} to {:?}]. \
                    You might need to rediscover the service, check https://docs.restate.dev/references/errors/#RT0015",
                    Version::V7,
                    Version::maximum_supported_version()
                ),
            ));
        }

        Ok(Self {
            decoder: Decoder::new(version),
            context: Context {
                start_info: None,
                journal: Default::default(),
                negotiated_protocol_version: version,
                input_is_closed: false,
                output: Output::new(version),
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
                    if self.do_transition(HitError(e.into())).is_err() {
                        return;
                    }
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

    fn is_ready_to_execute(&self) -> VMResult<bool> {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingEarlierEntries { .. }) => Ok(false),
            Ok(State::Processing { .. }) => Ok(true),
            Ok(s) => Err(s.as_unexpected_state("IsReadyToExecute")),
            Err(e) => Err(e.clone()),
        }
    }

    fn get_input(&mut self) -> VMResult<Input> {
        self.do_transition(GetInput)
    }

    fn take_next_notification(&mut self) -> VMResult<Option<Notification>> {
        self.do_transition(TakeNextNotification)
    }

    fn cmd_state_get(
        &mut self,
        key: String,
        cmd_name: Option<String>,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateGet(key, cmd_name))
    }

    fn cmd_state_get_keys(&mut self, cmd_name: Option<String>) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateGetKeys(cmd_name))
    }

    fn cmd_state_set(
        &mut self,
        key: String,
        value: Bytes,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateSet(key, value, cmd_name, cmd_stored_ack))
    }

    fn cmd_state_clear(
        &mut self,
        key: String,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateClear(key, cmd_name, cmd_stored_ack))
    }

    fn cmd_state_clear_all(
        &mut self,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdStateClearAll(cmd_name, cmd_stored_ack))
    }

    fn cmd_call(
        &mut self,
        target: Target,
        input: Bytes,
        cmd_name: Option<String>,
    ) -> VMResult<CallHandle> {
        self.verify_target(&target)?;
        self.do_transition(CmdCall(target, input, cmd_name))
    }

    fn cmd_send(
        &mut self,
        target: Target,
        input: Bytes,
        execute_at: Option<Duration>,
        cmd_name: Option<String>,
    ) -> VMResult<SendHandle> {
        self.verify_target(&target)?;
        let invoke_time = match u64::try_from(execute_at.unwrap_or_default().as_millis()) {
            Ok(invoke_time) => invoke_time,
            Err(e) => {
                self.do_transition(HitError(
                    OutOfBoundsDuration("send execution time", e).into(),
                ))?;
                unreachable!();
            }
        };
        self.do_transition(CmdSend(target, input, invoke_time, cmd_name))
    }

    fn cmd_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
        cmd_name: Option<String>,
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
        self.do_transition(CmdAttachInvocation(target, cmd_name))
    }

    fn cmd_cancel_invocation(
        &mut self,
        invocation_id: String,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle> {
        self.do_transition(CmdCancelInvocation(invocation_id, cmd_name, cmd_stored_ack))
    }

    fn trace_start(&mut self, name: String, parent: Option<TraceId>) -> VMResult<TraceId> {
        self.do_transition(TraceStart(name, parent))
    }

    fn trace_end(&mut self, trace: TraceId, result: NonEmptyValue) -> VMResult<()> {
        self.do_transition(TraceEnd(trace, result))
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

    fn state(&self) -> AttemptState {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingEarlierEntries { .. }) => {
                AttemptState::Starting
            }
            Ok(State::Processing { .. }) => AttemptState::Running,
            Ok(State::Closed) | Err(_) => AttemptState::Closed,
        }
    }
}
