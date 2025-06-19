pub mod error;
mod headers;
#[cfg(feature = "request_identity")]
mod request_identity;
mod retries;
mod service_protocol;
mod vm;

use bytes::Bytes;
use std::borrow::Cow;
use std::time::Duration;

pub use crate::retries::RetryPolicy;
pub use error::Error;
pub use headers::HeaderMap;
#[cfg(feature = "request_identity")]
pub use request_identity::*;
pub use service_protocol::Version;
pub use vm::CoreVM;

#[derive(Debug, Eq, PartialEq)]
pub struct Header {
    pub key: Cow<'static, str>,
    pub value: Cow<'static, str>,
}

#[derive(Debug)]
pub struct ResponseHead {
    pub status_code: u16,
    pub headers: Vec<Header>,
    pub version: Version,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Input {
    pub invocation_id: String,
    pub random_seed: u64,
    pub key: String,
    pub headers: Vec<Header>,
    pub input: Bytes,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CommandType {
    Input,
    Output,
    GetState,
    GetStateKeys,
    SetState,
    ClearState,
    ClearAllState,
    GetPromise,
    PeekPromise,
    CompletePromise,
    Sleep,
    Call,
    OneWayCall,
    SendSignal,
    RunCommand,
    AttachInvocation,
    GetInvocationOutput,
    CompleteAwakeable,
}

/// Used in `notify_error` to specify which command this error relates to.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CommandRelationship {
    Last,
    Next {
        ty: CommandType,
        name: Option<Cow<'static, str>>,
    },
    Specific {
        command_index: u32,
        ty: CommandType,
        name: Option<Cow<'static, str>>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub struct Target {
    pub service: String,
    pub handler: String,
    pub key: Option<String>,
    pub idempotency_key: Option<String>,
    pub headers: Vec<Header>,
}

pub const CANCEL_NOTIFICATION_HANDLE: NotificationHandle = NotificationHandle(1);

#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct NotificationHandle(u32);

impl From<u32> for NotificationHandle {
    fn from(value: u32) -> Self {
        NotificationHandle(value)
    }
}

impl From<NotificationHandle> for u32 {
    fn from(value: NotificationHandle) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CallHandle {
    pub invocation_id_notification_handle: NotificationHandle,
    pub call_notification_handle: NotificationHandle,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SendHandle {
    pub invocation_id_notification_handle: NotificationHandle,
}

#[derive(Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Value {
    /// a void/None/undefined success
    Void,
    Success(Bytes),
    Failure(TerminalFailure),
    /// Only returned for get_state_keys
    StateKeys(Vec<String>),
    /// Only returned for get_call_invocation_id
    InvocationId(String),
}

/// Terminal failure
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TerminalFailure {
    pub code: u16,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct EntryRetryInfo {
    /// Number of retries that happened so far for this entry.
    pub retry_count: u32,
    /// Time spent in the current retry loop.
    pub retry_loop_duration: Duration,
}

#[derive(Debug, Clone)]
pub enum RunExitResult {
    Success(Bytes),
    TerminalFailure(TerminalFailure),
    RetryableFailure {
        attempt_duration: Duration,
        error: Error,
    },
}

#[derive(Debug, Clone)]
pub enum NonEmptyValue {
    Success(Bytes),
    Failure(TerminalFailure),
}

impl From<NonEmptyValue> for Value {
    fn from(value: NonEmptyValue) -> Self {
        match value {
            NonEmptyValue::Success(s) => Value::Success(s),
            NonEmptyValue::Failure(f) => Value::Failure(f),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum AttachInvocationTarget {
    InvocationId(String),
    WorkflowId {
        name: String,
        key: String,
    },
    IdempotencyId {
        service_name: String,
        service_key: Option<String>,
        handler_name: String,
        idempotency_key: String,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum TakeOutputResult {
    Buffer(Bytes),
    EOF,
}

pub type VMResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum ImplicitCancellationOption {
    Disabled,
    Enabled {
        cancel_children_calls: bool,
        cancel_children_one_way_calls: bool,
    },
}

#[derive(Debug)]
pub struct VMOptions {
    pub implicit_cancellation: ImplicitCancellationOption,
}

impl Default for VMOptions {
    fn default() -> Self {
        Self {
            implicit_cancellation: ImplicitCancellationOption::Enabled {
                cancel_children_calls: true,
                cancel_children_one_way_calls: false,
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DoProgressResponse {
    /// Any of the given AsyncResultHandle completed
    AnyCompleted,
    /// The SDK should read from input at this point
    ReadFromInput,
    /// The SDK should execute a pending run
    ExecuteRun(NotificationHandle),
    /// Any of the run given before with ExecuteRun is waiting for completion
    WaitingPendingRun,
    /// Returned only when [ImplicitCancellationOption::Enabled].
    CancelSignalReceived,
}

pub trait VM: Sized {
    fn new(request_headers: impl HeaderMap, options: VMOptions) -> VMResult<Self>;

    fn get_response_head(&self) -> ResponseHead;

    // --- Input stream

    fn notify_input(&mut self, buffer: Bytes);

    fn notify_input_closed(&mut self);

    // --- Errors

    fn notify_error(&mut self, error: Error, related_command: Option<CommandRelationship>);

    // --- Output stream

    fn take_output(&mut self) -> TakeOutputResult;

    // --- Execution start waiting point

    fn is_ready_to_execute(&self) -> VMResult<bool>;

    // --- Async results

    fn is_completed(&self, handle: NotificationHandle) -> bool;

    fn do_progress(&mut self, any_handle: Vec<NotificationHandle>) -> VMResult<DoProgressResponse>;

    fn take_notification(&mut self, handle: NotificationHandle) -> VMResult<Option<Value>>;

    // --- Syscall(s)

    fn sys_input(&mut self) -> VMResult<Input>;

    fn sys_state_get(&mut self, key: String) -> VMResult<NotificationHandle>;

    fn sys_state_get_keys(&mut self) -> VMResult<NotificationHandle>;

    fn sys_state_set(&mut self, key: String, value: Bytes) -> VMResult<()>;

    fn sys_state_clear(&mut self, key: String) -> VMResult<()>;

    fn sys_state_clear_all(&mut self) -> VMResult<()>;

    /// Note: `now_since_unix_epoch` is only used for debugging purposes
    fn sys_sleep(
        &mut self,
        name: String,
        wake_up_time_since_unix_epoch: Duration,
        now_since_unix_epoch: Option<Duration>,
    ) -> VMResult<NotificationHandle>;

    fn sys_call(&mut self, target: Target, input: Bytes) -> VMResult<CallHandle>;

    fn sys_send(
        &mut self,
        target: Target,
        input: Bytes,
        execution_time_since_unix_epoch: Option<Duration>,
    ) -> VMResult<SendHandle>;

    fn sys_awakeable(&mut self) -> VMResult<(String, NotificationHandle)>;

    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()>;

    fn create_signal_handle(&mut self, signal_name: String) -> VMResult<NotificationHandle>;

    fn sys_complete_signal(
        &mut self,
        target_invocation_id: String,
        signal_name: String,
        value: NonEmptyValue,
    ) -> VMResult<()>;

    fn sys_get_promise(&mut self, key: String) -> VMResult<NotificationHandle>;

    fn sys_peek_promise(&mut self, key: String) -> VMResult<NotificationHandle>;

    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
    ) -> VMResult<NotificationHandle>;

    fn sys_run(&mut self, name: String) -> VMResult<NotificationHandle>;

    fn propose_run_completion(
        &mut self,
        notification_handle: NotificationHandle,
        value: RunExitResult,
        retry_policy: RetryPolicy,
    ) -> VMResult<()>;

    fn sys_cancel_invocation(&mut self, target_invocation_id: String) -> VMResult<()>;

    fn sys_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle>;

    fn sys_get_invocation_output(
        &mut self,
        target: AttachInvocationTarget,
    ) -> VMResult<NotificationHandle>;

    fn sys_write_output(&mut self, value: NonEmptyValue) -> VMResult<()>;

    fn sys_end(&mut self) -> VMResult<()>;

    // Returns true if the state machine is waiting pre-flight to complete
    fn is_waiting_preflight(&self) -> bool;

    // Returns true if the state machine is replaying
    fn is_replaying(&self) -> bool;

    /// Returns true if the state machine is in processing state
    fn is_processing(&self) -> bool;

    /// Returns last command index. Returns `-1` if there was no progress in the journal.
    fn last_command_index(&self) -> i64;
}

// HOW TO USE THIS API
//
// pre_user_code:
//     while !vm.is_ready_to_execute() {
//         match io.read_input() {
//             buffer => vm.notify_input(buffer),
//             EOF => vm.notify_input_closed()
//         }
//     }
//
// sys_[something]:
//     try {
//         vm.sys_[something]()
//         io.write_out(vm.take_output())
//     } catch (e) {
//         log(e)
//         io.write_out(vm.take_output())
//         throw e
//     }
//
// await_restate_future:
//     vm.notify_await_point(handle);
//     loop {
//         // Result here can be value, not_ready, suspended, vm error
//         let result = vm.take_async_result(handle);
//         if result.is_not_ready() {
//             match await io.read_input() {
//                buffer => vm.notify_input(buffer),
//                EOF => vm.notify_input_closed()
//             }
//         }
//         return result
//     }
//
// post_user_code:
//     // Consume vm.take_output() until EOF
//     while buffer = vm.take_output() {
//         io.write_out(buffer)
//     }
//     io.close()

#[cfg(test)]
mod tests;
