use crate::headers::HeaderMap;
use crate::service_protocol::messages::{
    attach_invocation_command_message, complete_awakeable_command_message,
    complete_promise_command_message, get_invocation_output_command_message,
    output_command_message, send_signal_command_message, AttachInvocationCommandMessage,
    CallCommandMessage, ClearAllStateCommandMessage, ClearStateCommandMessage,
    CompleteAwakeableCommandMessage, CompletePromiseCommandMessage,
    GetInvocationOutputCommandMessage, GetPromiseCommandMessage, IdempotentRequestTarget,
    OneWayCallCommandMessage, OutputCommandMessage, PeekPromiseCommandMessage,
    SendSignalCommandMessage, SetStateCommandMessage, SleepCommandMessage, WorkflowTarget,
};
use crate::service_protocol::{Decoder, NotificationId, RawMessage, Version, CANCEL_SIGNAL_ID};
use crate::vm::errors::{
    UnexpectedStateError, UnsupportedFeatureForNegotiatedVersion, EMPTY_IDEMPOTENCY_KEY,
};
use crate::vm::transitions::*;
use crate::{
    AttachInvocationTarget, CallHandle, DoProgressResponse, Error, Header, Input, NonEmptyValue,
    NotificationHandle, ResponseHead, RetryPolicy, RunExitResult, SendHandle, SuspendedOrVMError,
    TakeOutputResult, Target, VMOptions, VMResult, Value,
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
use tracing::{debug, enabled, instrument, Level};

mod context;
pub(crate) mod errors;
mod transitions;

const CONTENT_TYPE: &str = "content-type";

#[derive(Debug, IntoStaticStr)]
pub(crate) enum State {
    WaitingStart,
    WaitingReplayEntries {
        received_entries: u32,
        commands: VecDeque<RawMessage>,
        async_results: AsyncResultsState,
    },
    Replaying {
        commands: VecDeque<RawMessage>,
        run_state: RunState,
        async_results: AsyncResultsState,
    },
    Processing {
        processing_first_entry: bool,
        run_state: RunState,
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
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
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
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
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
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
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
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn is_completed(&self, handle: NotificationHandle) -> bool {
        match &self.last_transition {
            Ok(State::Replaying { async_results, .. })
            | Ok(State::Processing { async_results, .. }) => {
                async_results.is_handle_completed(handle)
            }
            _ => false,
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn do_progress(
        &mut self,
        any_handle: Vec<NotificationHandle>,
    ) -> Result<DoProgressResponse, SuspendedOrVMError> {
        match self.do_transition(DoProgress(any_handle)) {
            Ok(Ok(do_progress_response)) => Ok(do_progress_response),
            Ok(Err(suspended)) => Err(SuspendedOrVMError::Suspended(suspended)),
            Err(e) => Err(SuspendedOrVMError::VM(e)),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn take_notification(
        &mut self,
        handle: NotificationHandle,
    ) -> Result<Option<Value>, SuspendedOrVMError> {
        match self.do_transition(TakeNotification(handle)) {
            Ok(Ok(opt_value)) => Ok(opt_value),
            Ok(Err(suspended)) => Err(SuspendedOrVMError::Suspended(suspended)),
            Err(e) => Err(SuspendedOrVMError::VM(e)),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_state_get(&mut self, key: String) -> Result<NotificationHandle, Error> {
        invocation_debug_logs!(self, "Executing 'Get state {key}'");
        self.do_transition(SysStateGet(key))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_state_get_keys(&mut self) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Get state keys'");
        self.do_transition(SysStateGetKeys)
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_state_set(&mut self, key: String, value: Bytes) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Set state {key}'");
        self.context.eager_state.set(key.clone(), value.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysStateSet",
            SetStateCommandMessage {
                key: Bytes::from(key.into_bytes()),
                value: Some(value.into()),
                ..SetStateCommandMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_state_clear(&mut self, key: String) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear state {key}'");
        self.context.eager_state.clear(key.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysStateClear",
            ClearStateCommandMessage {
                key: Bytes::from(key.into_bytes()),
                ..ClearStateCommandMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_state_clear_all(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "Executing 'Clear all state'");
        self.context.eager_state.clear_all();
        self.do_transition(SysNonCompletableEntry(
            "SysStateClearAll",
            ClearAllStateCommandMessage::default(),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_sleep(
        &mut self,
        wake_up_time_since_unix_epoch: Duration,
        now_since_unix_epoch: Option<Duration>,
    ) -> VMResult<NotificationHandle> {
        if self.is_processing() {
            if let Some(now_since_unix_epoch) = now_since_unix_epoch {
                debug!(
                    "Executing 'Sleeping for {:?}'",
                    wake_up_time_since_unix_epoch - now_since_unix_epoch
                );
            } else {
                debug!("Executing 'Sleeping");
            }
        }

        let completion_id = self.context.journal.next_completion_notification_id();

        self.do_transition(SysSimpleCompletableEntry(
            "SysSleep",
            SleepCommandMessage {
                wake_up_time: u64::try_from(wake_up_time_since_unix_epoch.as_millis())
                    .expect("millis since Unix epoch should fit in u64"),
                result_completion_id: completion_id,
                ..Default::default()
            },
            completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_call(&mut self, target: Target, input: Bytes) -> VMResult<CallHandle> {
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

        let call_invocation_id_completion_id =
            self.context.journal.next_completion_notification_id();
        let result_completion_id = self.context.journal.next_completion_notification_id();

        let handles = self.do_transition(SysCompletableEntryWithMultipleCompletions(
            "SysCall",
            CallCommandMessage {
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
                invocation_id_notification_idx: call_invocation_id_completion_id,
                result_completion_id,
                ..Default::default()
            },
            vec![call_invocation_id_completion_id, result_completion_id],
        ))?;

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
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
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
        let call_invocation_id_completion_id =
            self.context.journal.next_completion_notification_id();
        let invocation_id_notification_handle = self.do_transition(SysSimpleCompletableEntry(
            "SysOneWayCall",
            OneWayCallCommandMessage {
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
                invocation_id_notification_idx: call_invocation_id_completion_id,
                ..Default::default()
            },
            call_invocation_id_completion_id,
        ))?;

        Ok(SendHandle {
            invocation_id_notification_handle,
        })
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_awakeable(&mut self) -> VMResult<(String, NotificationHandle)> {
        invocation_debug_logs!(self, "Executing 'Create awakeable'");

        let signal_id = self.context.journal.next_signal_notification_id();

        let handle = self.do_transition(CreateSignalHandle(
            "SysAwakeable",
            NotificationId::SignalId(signal_id),
        ))?;

        Ok((
            awakeable_id_str(&self.context.expect_start_info().id, signal_id),
            handle,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()> {
        invocation_debug_logs!(self, "Executing 'Complete awakeable {id}'");
        self.do_transition(SysNonCompletableEntry(
            "SysCompleteAwakeable",
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
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn create_signal_handle(&mut self, signal_name: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Create named signal'");

        self.do_transition(CreateSignalHandle(
            "SysCreateNamedSignal",
            NotificationId::SignalName(signal_name),
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
        self.do_transition(SysNonCompletableEntry(
            "SysCompleteAwakeable",
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
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_get_promise(&mut self, key: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Await promise {key}'");

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            "SysGetPromise",
            GetPromiseCommandMessage {
                key,
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_peek_promise(&mut self, key: String) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Peek promise {key}'");

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            "SysPeekPromise",
            PeekPromiseCommandMessage {
                key,
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Complete promise {key}'");

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            "SysCompletePromise",
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
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_run(&mut self, name: String) -> VMResult<NotificationHandle> {
        self.do_transition(SysRun(name))
    }

    #[instrument(
        level = "trace",
        skip(self, value, retry_policy),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
            match &value {
                RunExitResult::Success(_) => {
                    invocation_debug_logs!(self, "Journaling 'run' success result");
                }
                RunExitResult::TerminalFailure(_) => {
                    invocation_debug_logs!(self, "Journaling 'run' terminal failure result");
                }
                RunExitResult::RetryableFailure { .. } => {
                    invocation_debug_logs!(self, "Propagating 'run' failure");
                }
            }
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
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_cancel_invocation(&mut self, target_invocation_id: String) -> VMResult<()> {
        invocation_debug_logs!(
            self,
            "Executing 'Cancel invocation' of {target_invocation_id}"
        );
        self.verify_feature_support("cancel invocation", Version::V3)?;
        self.do_transition(SysNonCompletableEntry(
            "SysCancelInvocation",
            SendSignalCommandMessage {
                target_invocation_id,
                signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
                result: Some(send_signal_command_message::Result::Void(Default::default())),
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Attach invocation'");
        self.verify_feature_support("attach invocation", Version::V3)?;

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            "SysAttachInvocation",
            AttachInvocationCommandMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        attach_invocation_command_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::WorkflowId { name, key } => {
                        attach_invocation_command_message::Target::WorkflowTarget(WorkflowTarget {
                            workflow_name: name,
                            workflow_key: key,
                        })
                    }
                    AttachInvocationTarget::IdempotencyId {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                    } => attach_invocation_command_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                        },
                    ),
                }),
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_get_invocation_output(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle> {
        invocation_debug_logs!(self, "Executing 'Get invocation output'");
        self.verify_feature_support("get invocation output", Version::V3)?;

        let result_completion_id = self.context.journal.next_completion_notification_id();
        self.do_transition(SysSimpleCompletableEntry(
            "SysGetInvocationOutput",
            GetInvocationOutputCommandMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        get_invocation_output_command_message::Target::InvocationId(id)
                    }
                    AttachInvocationTarget::WorkflowId { name, key } => {
                        get_invocation_output_command_message::Target::WorkflowTarget(
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
                    } => get_invocation_output_command_message::Target::IdempotentRequestTarget(
                        IdempotentRequestTarget {
                            service_name,
                            service_key,
                            handler_name,
                            idempotency_key,
                        },
                    ),
                }),
                result_completion_id,
                ..Default::default()
            },
            result_completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, value),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
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
            OutputCommandMessage {
                result: Some(match value {
                    NonEmptyValue::Success(b) => output_command_message::Result::Value(b.into()),
                    NonEmptyValue::Failure(f) => output_command_message::Result::Failure(f.into()),
                }),
                ..OutputCommandMessage::default()
            },
        ))
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_end(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "End of the invocation");
        self.do_transition(SysEnd)
    }

    fn is_processing(&self) -> bool {
        matches!(&self.last_transition, Ok(State::Processing { .. }))
    }
}

const INDIFFERENT_PAD: GeneralPurposeConfig = GeneralPurposeConfig::new()
    .with_decode_padding_mode(DecodePaddingMode::Indifferent)
    .with_encode_padding(false);
const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, INDIFFERENT_PAD);

const AWAKEABLE_PREFIX: &str = "sign_1";

fn awakeable_id_str(id: &[u8], completion_index: u32) -> String {
    let mut input_buf = BytesMut::with_capacity(id.len() + size_of::<u32>());
    input_buf.put_slice(id);
    input_buf.put_u32(completion_index);
    format!("{AWAKEABLE_PREFIX}{}", URL_SAFE.encode(input_buf.freeze()))
}
