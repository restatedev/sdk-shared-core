use crate::headers::HeaderMap;
use crate::service_protocol::messages::get_state_keys_entry_message::StateKeys;
use crate::service_protocol::messages::{
    complete_awakeable_entry_message, complete_promise_entry_message, get_state_entry_message,
    get_state_keys_entry_message, output_entry_message, AwakeableEntryMessage, CallEntryMessage,
    ClearAllStateEntryMessage, ClearStateEntryMessage, CompleteAwakeableEntryMessage,
    CompletePromiseEntryMessage, Empty, GetPromiseEntryMessage, GetStateEntryMessage,
    GetStateKeysEntryMessage, OneWayCallEntryMessage, OutputEntryMessage, PeekPromiseEntryMessage,
    SetStateEntryMessage, SleepEntryMessage,
};
use crate::service_protocol::{Decoder, RawMessage, Version};
use crate::vm::context::{EagerGetState, EagerGetStateKeys};
use crate::vm::errors::UnexpectedStateError;
use crate::vm::transitions::*;
use crate::{
    AsyncResultCombinator, AsyncResultHandle, Error, Header, Input, NonEmptyValue, ResponseHead,
    RetryPolicy, RunEnterResult, RunExitResult, SuspendedOrVMError, TakeOutputResult, Target,
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

impl super::VM for CoreVM {
    #[instrument(level = "debug", skip_all, ret)]
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

        if version != Version::maximum_supported_version() {
            return Err(Error::new(
                errors::codes::UNSUPPORTED_MEDIA_TYPE,
                format!("Unsupported protocol version {:?}", version),
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
        level = "debug",
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
        level = "debug",
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn notify_input_closed(&mut self) {
        self.context.input_is_closed = true;
        let _ = self.do_transition(NotifyInputClosed);
    }

    #[instrument(
        level = "debug",
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
        level = "debug",
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
        level = "debug",
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn notify_await_point(&mut self, AsyncResultHandle(await_point): AsyncResultHandle) {
        let _ = self.do_transition(NotifyAwaitPoint(await_point));
    }

    #[instrument(
        level = "debug",
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_input(&mut self) -> Result<Input, Error> {
        self.do_transition(SysInput)
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_get(&mut self, key: String) -> Result<AsyncResultHandle, Error> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_get_keys(&mut self) -> VMResult<AsyncResultHandle> {
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
        level = "debug",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_set(&mut self, key: String, value: Bytes) -> Result<(), Error> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_clear(&mut self, key: String) -> Result<(), Error> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_state_clear_all(&mut self) -> Result<(), Error> {
        self.context.eager_state.clear_all();
        self.do_transition(SysNonCompletableEntry(
            "SysStateClearAll",
            ClearAllStateEntryMessage::default(),
        ))
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_sleep(&mut self, duration: Duration) -> VMResult<AsyncResultHandle> {
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
        level = "debug",
        skip(self, input),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_call(&mut self, target: Target, input: Bytes) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysCall",
            CallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                parameter: input,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "debug",
        skip(self, input),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_send(&mut self, target: Target, input: Bytes, delay: Option<Duration>) -> VMResult<()> {
        self.do_transition(SysNonCompletableEntry(
            "SysOneWayCall",
            OneWayCallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
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
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_awakeable(&mut self) -> VMResult<(String, AsyncResultHandle)> {
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
        level = "debug",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_get_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysGetPromise",
            GetPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_peek_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysPeekPromise",
            PeekPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

    #[instrument(
        level = "debug",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
    ) -> VMResult<AsyncResultHandle> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_run_enter(&mut self, name: String) -> Result<RunEnterResult, Error> {
        self.do_transition(SysRunEnter(name))
    }

    #[instrument(
        level = "debug",
        skip(self, value, retry_policy),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_run_exit(
        &mut self,
        value: RunExitResult,
        retry_policy: RetryPolicy,
    ) -> Result<AsyncResultHandle, Error> {
        self.do_transition(SysRunExit(value, retry_policy))
    }

    #[instrument(
        level = "debug",
        skip(self, value),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_write_output(&mut self, value: NonEmptyValue) -> Result<(), Error> {
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
        level = "debug",
        skip(self),
        fields(restate.invocation.id = self.debug_invocation_id(), restate.journal.index = self.context.journal.index(), restate.protocol.version = %self.version),
        ret
    )]
    fn sys_end(&mut self) -> Result<(), Error> {
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
        level = "debug",
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
