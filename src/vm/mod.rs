use crate::headers::HeaderMap;
use crate::service_protocol::messages::{
    complete_awakeable_entry_message, complete_promise_entry_message, get_state_entry_message,
    output_entry_message, AwakeableEntryMessage, CallEntryMessage, ClearAllStateEntryMessage,
    ClearStateEntryMessage, CompleteAwakeableEntryMessage, CompletePromiseEntryMessage, Empty,
    GetPromiseEntryMessage, GetStateEntryMessage, OneWayCallEntryMessage, OutputEntryMessage,
    PeekPromiseEntryMessage, SetStateEntryMessage, SleepEntryMessage,
};
use crate::service_protocol::{Decoder, RawMessage, Version};
use crate::vm::context::EagerGetState;
use crate::vm::errors::UnexpectedStateError;
use crate::vm::transitions::*;
use crate::{
    AsyncResultHandle, Header, Input, NonEmptyValue, ResponseHead, RunEnterResult,
    SuspendedOrVMError, TakeOutputResult, Target, VMError, VMResult, Value,
};
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{alphabet, Engine};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use context::{AsyncResultsState, Context, Output, RunState};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::mem::size_of;
use std::time::{Duration, SystemTime};
use strum::IntoStaticStr;
use tracing::instrument;

mod context;
pub(crate) mod errors;
mod transitions;

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
    fn as_unexpected_state(&self, event: &'static str) -> VMError {
        UnexpectedStateError::new(self.into(), event).into()
    }
}

pub struct CoreVM {
    version: Version,

    // Input decoder
    decoder: Decoder,

    // State machine
    context: Context,
    last_transition: Result<State, VMError>,
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
    fn new(request_headers: impl HeaderMap) -> Result<Self, VMError> {
        let version = request_headers
            .extract(CONTENT_TYPE)
            .map_err(|e| {
                VMError::new(
                    errors::codes::BAD_REQUEST,
                    format!("cannot read '{CONTENT_TYPE}' header: {e:?}"),
                )
            })?
            .ok_or(errors::MISSING_CONTENT_TYPE)?
            .parse::<Version>()?;

        Ok(Self {
            version,
            decoder: Decoder::new(version),
            context: Context {
                input_is_closed: false,
                output: Output::new(version),
                start_info: None,
                journal: Default::default(),
                eager_state: Default::default(),
            },
            last_transition: Ok(State::WaitingStart),
        })
    }

    #[instrument(level = "debug", ret)]
    fn get_response_head(&self) -> ResponseHead {
        ResponseHead {
            status_code: 200,
            headers: vec![Header {
                key: Cow::Borrowed(CONTENT_TYPE),
                value: Cow::Borrowed(self.version.content_type()),
            }],
        }
    }

    #[instrument(level = "debug", ret)]
    fn notify_input(&mut self, buffer: Vec<u8>) {
        self.decoder.push(buffer.into());
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

    #[instrument(level = "debug", ret)]
    fn notify_input_closed(&mut self) {
        self.context.input_is_closed = true;
        let _ = self.do_transition(NotifyInputClosed);
    }

    #[instrument(level = "debug", ret)]
    fn notify_error(&mut self, message: Cow<'static, str>, description: Cow<'static, str>) {
        let _ = self.do_transition(HitError(VMError {
            code: errors::codes::INTERNAL.into(),
            message,
            description,
        }));
    }

    #[instrument(level = "debug", ret)]
    fn take_output(&mut self) -> TakeOutputResult {
        if self.context.output.buffer.has_remaining() {
            TakeOutputResult::Buffer(
                self.context
                    .output
                    .buffer
                    .copy_to_bytes(self.context.output.buffer.remaining())
                    .to_vec(),
            )
        } else if !self.context.output.is_closed() {
            TakeOutputResult::Buffer(Vec::default())
        } else {
            TakeOutputResult::EOF
        }
    }

    #[instrument(level = "debug", ret)]
    fn is_ready_to_execute(&self) -> Result<bool, VMError> {
        match &self.last_transition {
            Ok(State::WaitingStart) | Ok(State::WaitingReplayEntries { .. }) => Ok(false),
            Ok(State::Processing { .. }) | Ok(State::Replaying { .. }) => Ok(true),
            Ok(s) => Err(UnexpectedStateError::new(s.into(), "IsReadyToExecute").into()),
            Err(e) => Err(e.clone()),
        }
    }

    #[instrument(level = "debug", ret)]
    fn notify_await_point(&mut self, AsyncResultHandle(await_point): AsyncResultHandle) {
        let _ = self.do_transition(NotifyAwaitPoint(await_point));
    }

    #[instrument(level = "debug", ret)]
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

    #[instrument(level = "debug", ret)]
    fn sys_input(&mut self) -> Result<Input, VMError> {
        self.do_transition(SysInput)
    }

    #[instrument(level = "debug", ret)]
    fn sys_get_state(&mut self, key: String) -> Result<AsyncResultHandle, VMError> {
        let result = match self.context.eager_state.get(&key) {
            EagerGetState::Unknown => None,
            EagerGetState::Empty => Some(get_state_entry_message::Result::Empty(Empty::default())),
            EagerGetState::Value(v) => Some(get_state_entry_message::Result::Value(v)),
        };
        self.do_transition(SysCompletableEntry(
            "SysGetState",
            GetStateEntryMessage {
                key: Bytes::from(key),
                result,
                ..Default::default()
            },
        ))
    }

    #[instrument(level = "debug", ret)]
    fn sys_set_state(&mut self, key: String, value: Vec<u8>) -> Result<(), VMError> {
        let value_buffer = Bytes::from(value);
        self.context
            .eager_state
            .set(key.clone(), value_buffer.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysSetState",
            SetStateEntryMessage {
                key: Bytes::from(key.into_bytes()),
                value: value_buffer,
                ..SetStateEntryMessage::default()
            },
        ))
    }

    #[instrument(level = "debug", ret)]
    fn sys_clear_state(&mut self, key: String) -> Result<(), VMError> {
        self.context.eager_state.clear(key.clone());
        self.do_transition(SysNonCompletableEntry(
            "SysClearState",
            ClearStateEntryMessage {
                key: Bytes::from(key.into_bytes()),
                ..ClearStateEntryMessage::default()
            },
        ))
    }

    #[instrument(level = "debug", ret)]
    fn sys_clear_all_state(&mut self) -> Result<(), VMError> {
        self.context.eager_state.clear_all();
        self.do_transition(SysNonCompletableEntry(
            "SysClearAllState",
            ClearAllStateEntryMessage::default(),
        ))
    }

    fn sys_sleep(&mut self, duration: Duration) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysSleep",
            SleepEntryMessage {
                wake_up_time: duration_to_wakeup_time(duration),
                ..Default::default()
            },
        ))
    }

    fn sys_call(&mut self, target: Target, input: Vec<u8>) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysCall",
            CallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                parameter: input.into(),
                ..Default::default()
            },
        ))
    }

    fn sys_send(
        &mut self,
        target: Target,
        input: Vec<u8>,
        delay: Option<Duration>,
    ) -> VMResult<()> {
        self.do_transition(SysNonCompletableEntry(
            "SysOneWayCall",
            OneWayCallEntryMessage {
                service_name: target.service,
                handler_name: target.handler,
                key: target.key.unwrap_or_default(),
                parameter: input.into(),
                invoke_time: delay.map(duration_to_wakeup_time).unwrap_or_default(),
                ..Default::default()
            },
        ))
    }

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

    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()> {
        self.do_transition(SysNonCompletableEntry(
            "SysCompleteAwakeable",
            CompleteAwakeableEntryMessage {
                id,
                result: Some(match value {
                    NonEmptyValue::Success(s) => {
                        complete_awakeable_entry_message::Result::Value(s.into())
                    }
                    NonEmptyValue::Failure(f) => {
                        complete_awakeable_entry_message::Result::Failure(f.into())
                    }
                }),
                ..Default::default()
            },
        ))
    }

    fn sys_get_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysGetPromise",
            GetPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

    fn sys_peek_promise(&mut self, key: String) -> VMResult<AsyncResultHandle> {
        self.do_transition(SysCompletableEntry(
            "SysPeekPromise",
            PeekPromiseEntryMessage {
                key,
                ..Default::default()
            },
        ))
    }

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
                        complete_promise_entry_message::Completion::CompletionValue(s.into())
                    }
                    NonEmptyValue::Failure(f) => {
                        complete_promise_entry_message::Completion::CompletionFailure(f.into())
                    }
                }),
                ..Default::default()
            },
        ))
    }

    #[instrument(level = "debug", ret)]
    fn sys_run_enter(&mut self, name: String) -> Result<RunEnterResult, VMError> {
        self.do_transition(SysRunEnter(name))
    }

    #[instrument(level = "debug", ret)]
    fn sys_run_exit(&mut self, value: NonEmptyValue) -> Result<AsyncResultHandle, VMError> {
        self.do_transition(SysRunExit(value))
    }

    #[instrument(level = "debug", ret)]
    fn sys_write_output(&mut self, value: NonEmptyValue) -> Result<(), VMError> {
        self.do_transition(SysNonCompletableEntry(
            "SysWriteOutput",
            OutputEntryMessage {
                result: Some(match value {
                    NonEmptyValue::Success(b) => {
                        output_entry_message::Result::Value(Bytes::from(b))
                    }
                    NonEmptyValue::Failure(f) => output_entry_message::Result::Failure(f.into()),
                }),
                ..OutputEntryMessage::default()
            },
        ))
    }

    #[instrument(level = "debug", ret)]
    fn sys_end(&mut self) -> Result<(), VMError> {
        self.do_transition(SysEnd)
    }
}

fn duration_to_wakeup_time(duration: Duration) -> u64 {
    u64::try_from(
        (SystemTime::now() + duration)
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("duration since Unix epoch should be well-defined")
            .as_millis(),
    )
    .expect("millis since Unix epoch should fit in u64")
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
