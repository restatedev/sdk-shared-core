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
    AttachInvocationTarget, CallHandle, DoProgressResponse, Error, Header,
    ImplicitCancellationOption, Input, NonEmptyValue, NotificationHandle, ResponseHead,
    RetryPolicy, RunExitResult, SendHandle, SuspendedOrVMError, TakeOutputResult, Target,
    TerminalFailure, VMOptions, VMResult, Value, CANCEL_NOTIFICATION_HANDLE,
};
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{alphabet, Engine};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use context::{AsyncResultsState, Context, Output, RunState};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::time::Duration;
use std::{fmt, mem};
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
    version: Version,
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
        any_handle: Vec<NotificationHandle>,
    ) -> Result<DoProgressResponse, SuspendedOrVMError> {
        match self.do_transition(DoProgress(any_handle)) {
            Ok(Ok(do_progress_response)) => Ok(do_progress_response),
            Ok(Err(suspended)) => Err(SuspendedOrVMError::Suspended(suspended)),
            Err(e) => Err(SuspendedOrVMError::VM(e)),
        }
    }

    fn is_implicit_cancellation_enabled(&self) -> bool {
        matches!(
            self.options.implicit_cancellation,
            ImplicitCancellationOption::Enabled { .. }
        )
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

        Ok(Self {
            version,
            options,
            decoder: Decoder::new(version),
            context: Context {
                input_is_closed: false,
                output: Output::new(version),
                start_info: None,
                journal: Default::default(),
                eager_state: Default::default(),
                next_retry_delay: None,
            },
            last_transition: Ok(State::WaitingStart),
            tracked_invocation_ids: vec![],
            sys_run_names: HashMap::with_capacity(0),
        })
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn do_progress(
        &mut self,
        mut any_handle: Vec<NotificationHandle>,
    ) -> Result<DoProgressResponse, SuspendedOrVMError> {
        if self.is_implicit_cancellation_enabled() {
            // We want the runtime to wake us up in case cancel notification comes in.
            any_handle.insert(0, CANCEL_NOTIFICATION_HANDLE);

            match self._do_progress(any_handle) {
                Ok(DoProgressResponse::AnyCompleted) => {
                    // If it's cancel signal, then let's go on with the cancellation logic
                    if self._is_completed(CANCEL_NOTIFICATION_HANDLE) {
                        // Loop once over the tracked invocation ids to resolve the unresolved ones
                        for i in 0..self.tracked_invocation_ids.len() {
                            if self.tracked_invocation_ids[i].is_resolved() {
                                continue;
                            }

                            let handle = self.tracked_invocation_ids[i].handle;

                            // Try to resolve it
                            match self._do_progress(vec![handle]) {
                                Ok(DoProgressResponse::AnyCompleted) => {
                                    let invocation_id = match self.do_transition(CopyNotification(handle)) {
                                        Ok(Ok(Some(Value::InvocationId(invocation_id)))) => Ok(invocation_id),
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
                            )
                            .map_err(SuspendedOrVMError::VM)?;
                        }

                        // Flip the cancellation
                        let _ = self.take_notification(CANCEL_NOTIFICATION_HANDLE);

                        // Done
                        Ok(DoProgressResponse::CancelSignalReceived)
                    } else {
                        Ok(DoProgressResponse::AnyCompleted)
                    }
                }
                res => res,
            }
        } else {
            self._do_progress(any_handle)
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
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
            Ok(Err(suspended)) => Err(SuspendedOrVMError::Suspended(suspended)),
            Err(e) => Err(SuspendedOrVMError::VM(e)),
        }
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
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
                        wake_up_time_since_unix_epoch - now_since_unix_epoch
                    );
                }
                (name, Some(now_since_unix_epoch)) => {
                    debug!(
                        "Executing 'Timer {name} with duration {:?}'",
                        wake_up_time_since_unix_epoch - now_since_unix_epoch
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

        let completion_id = self.context.journal.next_completion_notification_id();

        self.do_transition(SysSimpleCompletableEntry(
            "SysSleep",
            SleepCommandMessage {
                wake_up_time: u64::try_from(wake_up_time_since_unix_epoch.as_millis())
                    .expect("millis since Unix epoch should fit in u64"),
                result_completion_id: completion_id,
                name,
            },
            completion_id,
        ))
    }

    #[instrument(
        level = "trace",
        skip(self, input),
        fields(
            restate.invocation.id = self.debug_invocation_id(),
            restate.protocol.state = self.debug_state(),
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

        if matches!(
            self.options.implicit_cancellation,
            ImplicitCancellationOption::Enabled {
                cancel_children_calls: true,
                ..
            }
        ) {
            self.tracked_invocation_ids.push(TrackedInvocationId {
                handle: handles[0],
                invocation_id: None,
            })
        }

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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_run(&mut self, name: String) -> VMResult<NotificationHandle> {
        match self.do_transition(SysRun(name.clone())) {
            Ok(handle) => {
                if enabled!(Level::DEBUG) {
                    // Store the name, we need it later when completing
                    self.sys_run_names.insert(handle, name);
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
            let name: &str = self
                .sys_run_names
                .get(&notification_handle)
                .map(String::as_str)
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
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
            restate.protocol.state = self.debug_state(),
            restate.journal.command_index = self.context.journal.command_index(),
            restate.protocol.version = %self.version
        ),
        ret
    )]
    fn sys_end(&mut self) -> Result<(), Error> {
        invocation_debug_logs!(self, "End of the invocation");
        self.do_transition(SysEnd)
    }

    fn is_waiting_preflight(&self) -> bool {
        matches!(
            &self.last_transition,
            Ok(State::WaitingStart) | Ok(State::WaitingReplayEntries { .. })
        )
    }

    fn is_replaying(&self) -> bool {
        matches!(&self.last_transition, Ok(State::Replaying { .. }))
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
