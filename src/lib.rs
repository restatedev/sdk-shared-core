mod headers;
mod request_identity;
mod retries;
mod service_protocol;
mod vm;

use bytes::Bytes;
use std::borrow::Cow;
use std::time::Duration;

pub use crate::retries::RetryPolicy;
pub use headers::HeaderMap;
pub use request_identity::*;
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
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Suspended execution")]
pub struct SuspendedError;

#[derive(Debug, Clone, thiserror::Error)]
#[error("VM Error [{code}]: {message}. Description: {description}")]
pub struct VMError {
    code: u16,
    message: Cow<'static, str>,
    description: Cow<'static, str>,
}

impl VMError {
    pub fn new(code: impl Into<u16>, message: impl Into<Cow<'static, str>>) -> Self {
        VMError {
            code: code.into(),
            message: message.into(),
            description: Default::default(),
        }
    }

    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SuspendedOrVMError {
    #[error(transparent)]
    Suspended(SuspendedError),
    #[error(transparent)]
    VM(VMError),
}

#[derive(Debug, Eq, PartialEq)]
pub struct Input {
    pub invocation_id: String,
    pub random_seed: u64,
    pub key: String,
    pub headers: Vec<Header>,
    pub input: Bytes,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Target {
    pub service: String,
    pub handler: String,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AsyncResultHandle(u32);

impl From<u32> for AsyncResultHandle {
    fn from(value: u32) -> Self {
        AsyncResultHandle(value)
    }
}

impl From<AsyncResultHandle> for u32 {
    fn from(value: AsyncResultHandle) -> Self {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    /// a void/None/undefined success
    Void,
    Success(Bytes),
    Failure(Failure),
    /// Only returned for get_state_keys
    StateKeys(Vec<String>),
}

/// Terminal failure
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Failure {
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

#[derive(Debug)]
pub enum RunEnterResult {
    Executed(NonEmptyValue),
    NotExecuted(EntryRetryInfo),
}

#[derive(Debug, Clone)]
pub enum RunExitResult {
    Success(Bytes),
    TerminalFailure(Failure),
    RetryableFailure {
        attempt_duration: Duration,
        failure: Failure,
    },
}

#[derive(Debug, Clone)]
pub enum NonEmptyValue {
    Success(Bytes),
    Failure(Failure),
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
pub enum TakeOutputResult {
    Buffer(Bytes),
    EOF,
}

pub type VMResult<T> = Result<T, VMError>;

pub trait VM: Sized {
    fn new(request_headers: impl HeaderMap) -> VMResult<Self>;

    fn get_response_head(&self) -> ResponseHead;

    // --- Input stream

    fn notify_input(&mut self, buffer: Bytes);

    fn notify_input_closed(&mut self);

    // --- Errors

    fn notify_error(
        &mut self,
        message: Cow<'static, str>,
        description: Cow<'static, str>,
        next_retry_delay: Option<Duration>,
    );

    // --- Output stream

    fn take_output(&mut self) -> TakeOutputResult;

    // --- Execution start waiting point

    fn is_ready_to_execute(&self) -> Result<bool, VMError>;

    // --- Async results

    fn notify_await_point(&mut self, handle: AsyncResultHandle);

    /// Ok(None) means the result is not ready.
    fn take_async_result(
        &mut self,
        handle: AsyncResultHandle,
    ) -> Result<Option<Value>, SuspendedOrVMError>;

    // --- Syscall(s)

    fn sys_input(&mut self) -> VMResult<Input>;

    fn sys_state_get(&mut self, key: String) -> VMResult<AsyncResultHandle>;

    fn sys_state_get_keys(&mut self) -> VMResult<AsyncResultHandle>;

    fn sys_state_set(&mut self, key: String, value: Bytes) -> VMResult<()>;

    fn sys_state_clear(&mut self, key: String) -> VMResult<()>;

    fn sys_state_clear_all(&mut self) -> VMResult<()>;

    fn sys_sleep(&mut self, wake_up_time_since_unix_epoch: Duration)
        -> VMResult<AsyncResultHandle>;

    fn sys_call(&mut self, target: Target, input: Bytes) -> VMResult<AsyncResultHandle>;

    fn sys_send(
        &mut self,
        target: Target,
        input: Bytes,
        execution_time_since_unix_epoch: Option<Duration>,
    ) -> VMResult<()>;

    fn sys_awakeable(&mut self) -> VMResult<(String, AsyncResultHandle)>;

    fn sys_complete_awakeable(&mut self, id: String, value: NonEmptyValue) -> VMResult<()>;

    fn sys_get_promise(&mut self, key: String) -> VMResult<AsyncResultHandle>;

    fn sys_peek_promise(&mut self, key: String) -> VMResult<AsyncResultHandle>;

    fn sys_complete_promise(
        &mut self,
        key: String,
        value: NonEmptyValue,
    ) -> VMResult<AsyncResultHandle>;

    fn sys_run_enter(&mut self, name: String) -> VMResult<RunEnterResult>;

    fn sys_run_exit(
        &mut self,
        value: RunExitResult,
        retry_policy: RetryPolicy,
    ) -> VMResult<AsyncResultHandle>;

    fn sys_write_output(&mut self, value: NonEmptyValue) -> VMResult<()>;

    fn sys_end(&mut self) -> VMResult<()>;

    /// Returns true if the state machine is in processing state
    fn is_processing(&self) -> bool;

    /// Returns true if the state machine is between a sys_run_enter and sys_run_exit
    fn is_inside_run(&self) -> bool;
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
