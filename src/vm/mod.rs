use crate::headers::HeaderMap;
use crate::service_protocol::messages::get_state_keys_entry_message::StateKeys;
use crate::service_protocol::messages::{
    attach_invocation_entry_message, cancel_invocation_entry_message,
    complete_awakeable_entry_message, complete_promise_entry_message,
    get_invocation_output_entry_message, get_state_entry_message, get_state_keys_entry_message,
    output_entry_message, AttachInvocationEntryMessage, AwakeableEntryMessage, CallEntryMessage,
    CancelInvocationEntryMessage, ClearAllStateEntryMessage, ClearStateEntryMessage,
    CompleteAwakeableEntryMessage, CompletePromiseEntryMessage, Empty,
    GetCallInvocationIdEntryMessage, GetInvocationOutputEntryMessage, GetPromiseEntryMessage,
    GetStateEntryMessage, GetStateKeysEntryMessage, IdempotentRequestTarget,
    OneWayCallEntryMessage, OutputEntryMessage, PeekPromiseEntryMessage, SetStateEntryMessage,
    SleepEntryMessage, WorkflowTarget,
};
use crate::service_protocol::{Decoder, RawMessage, Version};
use crate::vm::context::{EagerGetState, EagerGetStateKeys};
use crate::vm::errors::{
    UnexpectedStateError, UnsupportedFeatureForNegotiatedVersion, EMPTY_IDEMPOTENCY_KEY,
};
use crate::vm::transitions::*;
use crate::{
    AsyncResultCombinator, AsyncResultHandle, AttachInvocationTarget, CancelInvocationTarget,
    Error, GetInvocationIdTarget, Header, Input, NonEmptyValue, ResponseHead, RetryPolicy,
    RunEnterResult, RunExitResult, SendHandle, SuspendedOrVMError, TakeOutputResult, Target,
    VMOptions, VMResult, Value,
};
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{alphabet, Engine};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use context::{AsyncResultsState, Context, Output, RunState};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::mem::size_of;
use std::time::Duration;
use strum::IntoStaticStr;
use tracing::instrument;

mod context;
pub(crate) mod errors;
mod transitions;

pub(crate) use transitions::AsyncResultAccessTrackerInner;

const CONTENT_TYPE: &str = "content-type";

#[derive(Debug, IntoStaticStr)]
pub(crate) enum State {
    WaitingStart,
    WaitingReplayEntries {
        entries: VecDeque<RawMessage>,
        async_results: AsyncResultsState,
    },
    Replaying {
        current_await_point: Option<u32>,
        entries: VecDeque<RawMessage>,
        async_results: AsyncResultsState,
    },
    Processing {
        run_state: RunState,
        current_await_point: Option<u32>,
        async_results: AsyncResultsState,
    },
    Ended,
    Suspended,
}

impl State {
    fn as_unexpected_state(&self, event: &'static str) -> Error {
        UnexpectedStateError::new(self.into(), event).into()
    }
}

pub struct CoreVM {
    version: Version,

    // Input decoder
    decoder: Decoder,

    // State machine
    context: Context,
    last_transition: Result<State, Error>,
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

    fn verify_feature_support(
        &mut self,
        feature: &'static str,
        minimum_required_protocol: Version,
    ) -> VMResult<()> {
        if self.version < minimum_required_protocol {
            return self.do_transition(HitError {
                error: UnsupportedFeatureForNegotiatedVersion::new(
                    feature,
                    self.version,
                    minimum_required_protocol,
                )
                .into(),
                next_retry_delay: None,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for CoreVM {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("CoreVM");
        s.field("version", &self.version);

        if let Some(start_info) = self.context.start_info() {
            s.field("invocation_id", &start_info.debug_id);
        }

        match &self.last_transition {
            Ok(state) => s.field("last_transition", &<&'static str>::from(state)),
            Err(_) => s.field("last_transition", &"Errored"),
        };

        s.field("execution_index", &self.context.journal.index())
            .finish()
    }
}

// --- Bound checks
#[allow(unused)]
const fn is_send<T: Send>() {}
const _: () = is_send::<CoreVM>();

// Macro used for informative debug logs
macro_rules! invocation_debug_logs {
    ($this:expr, $($arg:tt)*) => {
        if ($this.is_processing()) {
            tracing::debug!($($arg)*)
        }
    };
}

impl super::VM for CoreVM {
    #[instrument(level = "trace", skip_all, ret)]
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
                    "Unsupported protocol version {:?}. Supported versions: {:?} to {:?}",
                    version,
                    Version::minimum_supported_version(),
                    Version::maximum_supported_version()
                ),
            ));
        }

        Ok(Self {
            version,
            decoder: Decoder::new(version),
            context: Context {
                input_is_closed: false,
                output: Output::new(version),
                start_info: None,
                journal: Default::default(),
                eager_state: Default::default(),
                next_retry_delay: None,
                options,
            },
            last_transition: Ok(State::WaitingStart),
        })
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn get_response_head(&self) -> ResponseHead {
        ResponseHead {
            status_code: 200,
            headers: vec![Header {
                key: Cow::Borrowed(CONTENT_TYPE),
                value: Cow::Borrowed(self.version.content_type()),
            }],
            version: self.version,
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
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
                    if self
                        .do_transition(HitError {
                            error: e.into(),
                            next_retry_delay: None,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn notify_input_closed(&mut self) {
        self.context.input_is_closed = true;
        let _ = self.do_transition(NotifyInputClosed);
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn notify_error(&mut self, error: Error, next_retry_delay: Option<Duration>) {
        let _ = self.do_transition(HitError {
            error,
            next_retry_delay,
        });
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn take_output(&mut self) -> TakeOutputResult {
        if self.context.output.buffer.has_remaining() {
            TakeOutputResult::Buffer(
                self.context
                    .output
                    .buffer
                    .copy_to_bytes(self.context.output.buffer.remaining()),
            )
        } else if !self.context.output.is_closed() {
            TakeOutputResult::Buffer(Bytes::default())
        } else {
            TakeOutputResult::EOF
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn is_ready_to_execute(&self) -> Result<bool, Error> {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingReplayEntries { .. }) => Ok(false),
            Ok(State::Processing { .. }) | Ok(State::Replaying { .. }) => Ok(true),
            Ok(s) => Err(UnexpectedStateError::new(s.into(), "IsReadyToExecute").into()),
            Err(e) => Err(e.clone()),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn notify_await_point(&mut self, AsyncResultHandle(await_point): AsyncResultHandle) {
        let _ = self.do_transition(NotifyAwaitPoint(await_point));
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn take_async_result(
        &mut self,
        handle: AsyncResultHandle,
    ) -> Result<Option<Value>, SuspendedOrVMError> {
        match self.do_transition(TakeAsyncResult(handle.0)) {
            Ok(Ok(opt_value)) => Ok(opt_value),
            Ok(Err(suspended)) => Err(SuspendedOrVMError::Suspended(suspended)),
            Err(e) => Err(SuspendedOrVMError::VM(e)),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_input(&mut self) -> Result<Input, Error> {
        self.do_transition(SysInput)
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_get(&mut self, key: String) -> Result<AsyncResultHandle, Error> {
        invocation_debug_logs!(self, "Executing 'Get state {key}'");
        let result = match self.context.eager_state.get(&key) {
            EagerGetState::Unknown => None,
            EagerGetState::Empty => Some(get_state_entry_message::Result::Empty(Empty::default())),
            EagerGetState::Value(v) => Some(get_state_entry_message::Result::Value(v)),
        };
        self.do_transition(SysCompletableEntry(
            "SysStateGet",
            GetStateEntryMessage {
                key: Bytes::from(key),
                result,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_get_keys(&mut self) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Get state keys'");
        let result = match self.context.eager_state.get_keys() {
            EagerGetStateKeys::Unknown => None,
            EagerGetStateKeys::Keys(keys) => {
                Some(get_state_keys_entry_message::Result::Value(StateKeys {
                    keys: keys.into_iter().map(Bytes::from).collect(),
                }))
            }
        };
        self.do_transition(SysCompletableEntry(
            "SysStateGetKeys",
            GetStateKeysEntryMessage {
                result,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_set(&mut self, key: String, value: Bytes) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Set state {key}'");
        self.context.eager_state.set(key.clone(), value.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysStateSet",
            SetStateEntryMessage {
                key: Bytes::from(key.into_bytes()),
                value,
                ..SetStateEntryMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_clear(&mut self, key: String) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear state {key}'");
        self.context.eager_state.clear(key.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysStateClear",
            ClearStateEntryMessage {
                key: Bytes::from(key.into_bytes()),
                ..ClearStateEntryMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_clear_all(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear all state keys'");
        self.context.eager_state.clear_all();
        self.do_transition(SysNonCompletableEntry(
            "SysStateClearAll",
            ClearAllStateEntryMessage::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_sleep(&mut self, duration: Duration) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Sleep for {duration:?}'");
        self.do_transition(SysCompletableEntry(
            "SysSleep",
            SleepEntryMessage {
                wake_up_time: u64::try_from(duration.as_millis())
                    .expect("millis since Unix epoch should fit in u64"),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_call(&mut self, target: Target, input: Bytes) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(
            self,
            "Executing 'Call {}/{}'",
            target.service,
            target.handler
        );
        if let Some(idempotency_key) = &target.idempotency_key {
            self.verify_feature_support("attach idempotency key to call", Version::V3)?;
            if idempotency_key.is_empty() {
                self.do_transition(HitError {
                    error: EMPTY_IDEMPOTENCY_KEY,
                    next_retry_delay: None,
                })?;
                unreachable!();
            }
        }
        self.do_transition(SysCompletableEntry(
            "SysCall",
            CallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                idempotency_key: target.idempotency_key,
                headers: target
                    .headers
                    .into_iter()
                    .map(crate::service_protocol::messages::Header::from)
                    .collect(),
                parameter: input,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_send(
        &mut self,
        target: Target,
        input: Bytes,
        delay: Option<Duration>,
    ) -> VMResult<SendHandle> {
        invocation_debug_logs!(
            self,
            "Executing 'Send to {}/{}'",
            target.service,
            target.handler
        );
        if let Some(idempotency_key) = &target.idempotency_key {
            self.verify_feature_support("attach idempotency key to one way call", Version::V3)?;
            if idempotency_key.is_empty() {
                self.do_transition(HitError {
                    error: EMPTY_IDEMPOTENCY_KEY,
                    next_retry_delay: None,
                })?;
                unreachable!();
            }
        }
        self.do_transition(SysNonCompletableEntry(
            "SysOneWayCall",
            OneWayCallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                idempotency_key: target.idempotency_key,
                headers: target
                    .headers
                    .into_iter()
                    .map(crate::service_protocol::messages::Header::from)
                    .collect(),
                parameter: input,
                invoke_time: delay
                    .map(|d| {
                        u64::try_from(d.as_millis())
                            .expect("millis since Unix epoch should fit in u64")
                    })
                    .unwrap_or_default(),
                ..Default::default()
            },
        ))
        .map(|_| SendHandle(self.context.journal.expect_index()))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_awakeable(&mut self) -> VMResult<(String, AsyncResultHandle)> {
        invocation_debug_logs!(self, "Executing 'Awakeable'");
        self.do_transition(SysCompletableEntry(
            "SysAwakeable",
            AwakeableEntryMessage::default(),
        ))
        .map(|h| {
            (
                awakeable_id(
                    &self.context.expect_start_info().id,
                    self.context.journal.expect_index(),
                ),
                h,
            )
        })
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Complete awakeable {id}'");
        self.do_transition(SysNonCompletableEntry(
            "SysCompleteAwakeable",
            CompleteAwakeableEntryMessage {
                id,
                result: Some(match value {
                    NonEmptyValue::Success(s) => complete_awakeable_entry_message::Result::Value(s),
                    NonEmptyValue::Failure(f) => {
                        complete_awakeable_entry_message::Result::Failure(f.into())
                    }
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_get_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Await promise {key}'");
        self.do_transition(SysCompletableEntry(
            "SysGetPromise",
            GetPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_peek_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Peek promise {key}'");
        self.do_transition(SysCompletableEntry(
            "SysPeekPromise",
            PeekPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
    ) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Complete promise {key}'");
        self.do_transition(SysCompletableEntry(
            "SysCompletePromise",
            CompletePromiseEntryMessage {
                key,
                completion: Some(match value {
                    NonEmptyValue::Success(s) => {
                        complete_promise_entry_message::Completion::CompletionValue(s)
                    }
                    NonEmptyValue::Failure(f) => {
                        complete_promise_entry_message::Completion::CompletionFailure(f.into())
                    }
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_run_enter(&mut self, name: String) -> Result<RunEnterResult, Error> {
        self.do_transition(SysRunEnter(name))
    }

    #[instrument(
        level = "trace",
        skip(self, value, retry_policy),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_run_exit(
        &mut self,
        value: RunExitResult,
        retry_policy: RetryPolicy,
    ) -> Result<AsyncResultHandle, Error> {
        match &value {
            RunExitResult::Success(_) => {
                invocation_debug_logs!(self, "Storing side effect completed with success");
            }
            RunExitResult::TerminalFailure(_) => {
                invocation_debug_logs!(self, "Storing side effect completed with terminal failure");
            }
            RunExitResult::RetryableFailure { .. } => {
                invocation_debug_logs!(self, "Propagating side effect failure");
            }
        }
        self.do_transition(SysRunExit(value, retry_policy))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_get_call_invocation_id(
        &mut self,
        target: GetInvocationIdTarget,
    ) -> VMResult<AsyncResultHandle> {
        invocation_debug_logs!(self, "Executing 'Get invocation id'");
        self.verify_feature_support("get call invocation id", Version::V3)?;
        self.do_transition(SysCompletableEntry(
            "SysGetCallInvocationId",
            GetCallInvocationIdEntryMessage {
                call_entry_index: match target {
                    GetInvocationIdTarget::CallEntry(h) => h.0,
                    GetInvocationIdTarget::SendEntry(h) => h.0,
                },
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_cancel_invocation(&mut self, target: CancelInvocationTarget) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Cancel invocation'");
        self.verify_feature_support("cancel invocation", Version::V3)?;
        self.do_transition(SysNonCompletableEntry(
            "SysCancelInvocation",
            CancelInvocationEntryMessage {
                target: Some(match target {
                    CancelInvocationTarget::InvocationId(id) => {
                        cancel_invocation_entry_message::Target::InvocationId(id)
                    }
                    CancelInvocationTarget::CallEntry(handle) => {
                        cancel_invocation_entry_message::Target::CallEntryIndex(handle.0)
                    }
                    CancelInvocationTarget::SendEntry(handle) => {
                        cancel_invocation_entry_message::Target::CallEntryIndex(handle.0)
                    }
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_attach_invocation(&mut self, target: AttachInvocationTarget) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Attach invocation'");
        self.verify_feature_support("attach invocation", Version::V3)?;
        self.do_transition(SysNonCompletableEntry(
            "SysAttachInvocation",
            AttachInvocationEntryMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        attach_invocation_entry_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::CallEntry(handle) => {
                        attach_invocation_entry_message::Target::CallEntryIndex(handle.0)
                    }
                    AttachInvocationTarget::SendEntry(handle) => {
                        attach_invocation_entry_message::Target::CallEntryIndex(handle.0)
                    }
                    AttachInvocationTarget::WorkflowId { name, key } => {
                        attach_invocation_entry_message::Target::WorkflowTarget(WorkflowTarget {
                            workflow_name: name,
                            workflow_key: key,
                        })
                    }
                    AttachInvocationTarget::IdempotencyId {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                    } => attach_invocation_entry_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                        },
                    ),
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_get_invocation_output(&mut self, target: AttachInvocationTarget) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Get invocation output'");
        self.verify_feature_support("get invocation output", Version::V3)?;
        self.do_transition(SysNonCompletableEntry(
            "SysGetInvocationOutput",
            GetInvocationOutputEntryMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        get_invocation_output_entry_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::CallEntry(handle) => {
                        get_invocation_output_entry_message::Target::CallEntryIndex(handle.0)
                    }
                    AttachInvocationTarget::SendEntry(handle) => {
                        get_invocation_output_entry_message::Target::CallEntryIndex(handle.0)
                    }
                    AttachInvocationTarget::WorkflowId { name, key } => {
                        get_invocation_output_entry_message::Target::WorkflowTarget(
                            WorkflowTarget {
                                workflow_name: name,
                                workflow_key: key,
                            },
                        )
                    }
                    AttachInvocationTarget::IdempotencyId {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                    } => get_invocation_output_entry_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                        },
                    ),
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_write_output(&mut self, value: NonEmptyValue) -> Result<(), Error> {
        match &value {
            NonEmptyValue::Success(_) => {
                invocation_debug_logs!(self, "Writing invocation result success value");
            }
            NonEmptyValue::Failure(_) => {
                invocation_debug_logs!(self, "Writing invocation result failure value");
            }
        }
        self.do_transition(SysNonCompletableEntry(
            "SysWriteOutput",
            OutputEntryMessage {
                result: Some(match value {
                    NonEmptyValue::Success(b) => output_entry_message::Result::Value(b),
                    NonEmptyValue::Failure(f) => output_entry_message::Result::Failure(f.into()),
                }),
                ..OutputEntryMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_end(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "End of the invocation");
        self.do_transition(SysEnd)
    }

    fn is_processing(&self) -> bool {
        matches!(&self.last_transition, Ok(State::Processing { .. }))
    }

    fn is_inside_run(&self) -> bool {
        matches!(
            &self.last_transition,
            Ok(State::Processing {
                run_state: RunState::Running(_),
                ..
            })
        )
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_try_complete_combinator(
        &mut self,
        combinator: impl AsyncResultCombinator + fmt::Debug,
    ) -> VMResult<Option<AsyncResultHandle>> {
        self.do_transition(SysTryCompleteCombinator(combinator))
    }
}

const INDIFFERENT_PAD: GeneralPurposeConfig = GeneralPurposeConfig::new()
    .with_decode_padding_mode(DecodePaddingMode::Indifferent)
    .with_encode_padding(false);
const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, INDIFFERENT_PAD);

fn awakeable_id(id: &[u8], entry_index: u32) -> String {
    let mut input_buf = BytesMut::with_capacity(id.len() + size_of::<u32>());
    input_buf.put_slice(id);
    input_buf.put_u32(entry_index);
    format!("prom_1{}", URL_SAFE.encode(input_buf.freeze()))
}
