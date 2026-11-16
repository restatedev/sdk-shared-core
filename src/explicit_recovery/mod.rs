//! A state machine for handlers that are never replayed: every attempt runs the handler from the
//! start, and recovers explicitly from the state the earlier attempts stored, as session objects do.
//!
//! Unlike [`crate::VM`], there is no journal to replay, so there are no deterministic await
//! points either: an action returns a [`NotificationHandle`], and its result comes back as a [`Notification`]
//! with that handle. The SDK settles its own promises with them, and awaits them however it likes,
//! e.g. with the language's own combinators.
//!
//! # Driving the VM
//!
//! The VM is single threaded and does no I/O: every call is synchronous and only buffers output.
//!
//! 1. **Bootstrap.** Write [`ExplicitRecoveryVM::get_response_head`], then feed the request body with
//!    [`ExplicitRecoveryVM::notify_input`] until [`ExplicitRecoveryVM::is_ready_to_execute`], then call
//!    [`ExplicitRecoveryVM::get_input`] and start the handler.
//! 2. **Input pump.** One task per attempt keeps reading the request body: it is the only reader.
//!    It calls [`ExplicitRecoveryVM::notify_input`] for every chunk, and
//!    [`ExplicitRecoveryVM::notify_input_closed`] at the end of the body.
//! 3. **Ticks.** After every call into the VM (every context action, the input pump,
//!    [`ExplicitRecoveryVM::heartbeat`], [`ExplicitRecoveryVM::end`] and
//!    [`ExplicitRecoveryVM::notify_error`]), the SDK schedules a tick, at most one per event loop
//!    turn (e.g. `queueMicrotask` in JavaScript, `loop.call_soon` in Python). A tick:
//!    1. writes [`ExplicitRecoveryVM::take_output`] to the response body;
//!    2. drains [`ExplicitRecoveryVM::take_next_notification`], settling the promise of each handle;
//!    3. if [`ExplicitRecoveryVM::take_next_notification`] fails, the attempt is closed: with
//!       [`crate::Error::is_suspended_error`] it was shut down, otherwise it failed. The SDK aborts
//!       the handler's local work, rejects the promises still pending, and ends the response body;
//!    4. if [`ExplicitRecoveryVM::state`] is [`AttemptState::Closed`] (the handler ended), the SDK
//!       ends the response body.
//!
//! Scheduling a tick after every call batches the output of the actions issued in the same event
//! loop turn, and flushes it before the event loop can wait on anything.
//!
//! # The attempt
//!
//! The runtime shuts an attempt down by closing the input stream (in this version of the protocol,
//! after its inactivity timeout). From then on the attempt is over: every call returns an error
//! with [`crate::Error::is_suspended_error`], and nothing more is sent. The runtime starts a new
//! attempt once a response to a call or an attachment of the shut down attempt arrives, or right
//! away when there is none. A new attempt runs the handler from the start, and gets only the
//! responses of its own actions.
//!
//! Local work that may outlast the runtime's inactivity timeout (a model request, a long call)
//! must call [`ExplicitRecoveryVM::heartbeat`] meanwhile, or the attempt is shut down. A call that
//! can be issued again by a later attempt needs an idempotency key, or a callee that deduplicates.
//!
//! # Protocol
//!
//! This version runs on the service protocol V7 and V8:
//! * the entries of the earlier attempts are still sent to every attempt: the VM only uses them to
//!   number its own commands and completions after them, to end the traces they left open, and to
//!   know about a cancellation (see [`ExplicitRecoveryVM::is_cancelled`]);
//! * a state value not known locally is read with an ephemeral get on V8, and with a get recorded
//!   in the journal on V7;
//! * request/response transports (e.g. AWS Lambda, HTTP/1.1) are not supported: no response could
//!   reach the handler. An SDK serving one fails the invocation with
//!   [`request_response_unsupported`] (passed to [`ExplicitRecoveryVM::notify_error`] instead of
//!   calling [`ExplicitRecoveryVM::get_input`]), which pauses it.

mod vm;

pub use vm::{request_response_unsupported, CoreExplicitRecoveryVM};

use super::{
    AttachInvocationTarget, CallHandle, Error, HeaderMap, Input, NonEmptyValue, NotificationHandle,
    ResponseHead, SendHandle, Target, VMResult, Value,
};
use bytes::Bytes;
use std::time::Duration;

/// The result of the action that returned `handle`, see [`ExplicitRecoveryVM::take_next_notification`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Notification {
    pub handle: NotificationHandle,
    pub value: Value,
}

/// A trace started with [`ExplicitRecoveryVM::trace_start`].
#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct TraceId(u32);

impl From<u32> for TraceId {
    fn from(value: u32) -> Self {
        TraceId(value)
    }
}

impl From<TraceId> for u32 {
    fn from(value: TraceId) -> Self {
        value.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AttemptState {
    /// Receiving the start of the invocation, until [`ExplicitRecoveryVM::is_ready_to_execute`].
    Starting,
    /// The handler runs.
    Running,
    /// The attempt is over: the handler ended, the attempt was shut down, or it failed.
    /// [`ExplicitRecoveryVM::take_next_notification`] returns the error that closed it, if any.
    Closed,
}

/// State machine of an attempt of a handler that is never replayed. See the [module docs](self).
pub trait ExplicitRecoveryVM: Sized {
    /// Accepts the service protocol V7 and V8 only.
    fn new(request_headers: impl HeaderMap) -> VMResult<Self>;

    fn get_response_head(&self) -> ResponseHead;

    // --- Input and output

    fn notify_input(&mut self, buffer: Bytes);

    /// The runtime closed the input stream. While the handler runs, this shuts the attempt down.
    fn notify_input_closed(&mut self);

    /// Returns the bytes buffered so far for the response body.
    fn take_output(&mut self) -> Bytes;

    // --- Start

    fn is_ready_to_execute(&self) -> VMResult<bool>;

    // --- Results

    /// The invocation's input, once [`ExplicitRecoveryVM::is_ready_to_execute`].
    fn get_input(&mut self) -> VMResult<Input>;

    /// The next result, in the order they completed: `Ok(None)` when there is none.
    ///
    /// Once the attempt is closed, returns the error that closed it: a shutdown
    /// ([`crate::Error::is_suspended_error`]) or a failure. After the handler ended, `Ok(None)`.
    fn take_next_notification(&mut self) -> VMResult<Option<Notification>>;

    // --- Actions

    /// Completes with [`Value::Success`], or [`Value::Void`] when there is no value.
    ///
    /// A value written by this attempt, or sent by Restate at the start, completes at once,
    /// without asking Restate.
    fn cmd_state_get(
        &mut self,
        key: String,
        cmd_name: Option<String>,
    ) -> VMResult<NotificationHandle>;

    /// Completes with [`Value::StateKeys`].
    fn cmd_state_get_keys(&mut self, cmd_name: Option<String>) -> VMResult<NotificationHandle>;

    fn cmd_state_set(
        &mut self,
        key: String,
        value: Bytes,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle>;

    fn cmd_state_clear(
        &mut self,
        key: String,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle>;

    fn cmd_state_clear_all(
        &mut self,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle>;

    fn cmd_call(
        &mut self,
        target: Target,
        input: Bytes,
        cmd_name: Option<String>,
    ) -> VMResult<CallHandle>;

    /// `execute_at` is the time since the UNIX epoch at which to invoke the target.
    fn cmd_send(
        &mut self,
        target: Target,
        input: Bytes,
        execute_at: Option<Duration>,
        cmd_name: Option<String>,
    ) -> VMResult<SendHandle>;

    /// Completes with the result of the target invocation.
    fn cmd_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
        cmd_name: Option<String>,
    ) -> VMResult<NotificationHandle>;

    fn cmd_cancel_invocation(
        &mut self,
        invocation_id: String,
        cmd_name: Option<String>,
        cmd_stored_ack: bool,
    ) -> VMResult<NotificationHandle>;

    // -- Tracing

    /// Starts a trace, shown in Restate's UI until [`ExplicitRecoveryVM::trace_end`], inside the
    /// trace `parent` started by this attempt, or else inside the attempt itself.
    ///
    /// In this version of the protocol a trace is a run, and the run's name says its parent:
    /// `<name> (parent trace <parent>)`.
    ///
    /// A trace the attempt leaves open is ended with a failure by [`ExplicitRecoveryVM::end`], or by
    /// the next attempt.
    fn trace_start(&mut self, name: String, parent: Option<TraceId>) -> VMResult<TraceId>;

    fn trace_end(&mut self, trace: TraceId, result: NonEmptyValue) -> VMResult<()>;

    // --- Control

    /// Tells Restate the attempt makes progress, delaying its shutdown.
    fn heartbeat(&mut self) -> VMResult<()>;

    // --- End

    /// Ends the handler with its output: a value, or a terminal failure.
    fn end(&mut self, output: NonEmptyValue) -> VMResult<()>;

    /// Fails the attempt, for example with a retryable error of the handler.
    fn notify_error(&mut self, error: Error);

    /// Whether the invocation was cancelled: a cancellation signal arrived, in this attempt or in
    /// an earlier one.
    fn is_cancelled(&self) -> bool;

    fn state(&self) -> AttemptState;
}
