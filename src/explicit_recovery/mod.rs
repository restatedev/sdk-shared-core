//! A state machine for handlers on the explicit recovery protocol, version 1: handlers with no
//! journal. Every attempt runs the handler from its first line, and recovers from the state the
//! earlier attempts stored. Nothing an earlier attempt sent or received is replayed.
//!
//! An action returns a [`NotificationHandle`] (or nothing, when nobody wants its result), and its
//! result comes back as a [`Notification`] with that handle, from
//! [`ExplicitRecoveryVM::take_next_notification`]. The SDK settles its own promises with them, and
//! awaits them however it likes.
//!
//! # Driving the VM
//!
//! The VM is single threaded and does no I/O: every call is synchronous and only buffers output.
//!
//! 1. **Bootstrap.** Build it with [`ExplicitRecoveryVM::new`] from the request headers. If that
//!    fails, answer the request with the error's code as HTTP status (415 for a content type the
//!    VM does not speak) and no body. Otherwise write [`ExplicitRecoveryVM::get_response_head`],
//!    then feed the request body with [`ExplicitRecoveryVM::notify_input`] until
//!    [`ExplicitRecoveryVM::is_ready_to_execute`], call [`ExplicitRecoveryVM::get_input`] and
//!    start the handler. `StartMessage` is all it waits for.
//! 2. **Input pump.** One task per attempt reads the request body: it is the only reader. It
//!    calls [`ExplicitRecoveryVM::notify_input`] for every chunk, and
//!    [`ExplicitRecoveryVM::notify_input_closed`] at the end of the body.
//! 3. **Ticks.** After every call into the VM, the SDK schedules a tick, at most one per event
//!    loop turn. A tick:
//!    1. writes [`ExplicitRecoveryVM::take_output`] to the response body;
//!    2. drains [`ExplicitRecoveryVM::take_next_notification`], settling the promise of each
//!       handle before it yields. Taking a notification **is** processing it: it counts in
//!       `ShutdownMessage`. Never take one to keep it for later. The cancel signal comes on
//!       [`CANCEL_NOTIFICATION_HANDLE`]; after it, [`ExplicitRecoveryVM::is_cancelled`] is true;
//!    3. if [`ExplicitRecoveryVM::take_next_notification`] fails, the attempt is over: with
//!       [`crate::Error::is_suspended_error`] it was shut down, otherwise it failed. The SDK
//!       stops the handler's local work and rejects the promises still pending;
//!    4. once [`ExplicitRecoveryVM::should_close_output`] is true (it wrote its last message
//!       already), the SDK ends the response body.
//!
//! # How an attempt ends
//!
//! - **The handler returns**: [`ExplicitRecoveryVM::end`] writes `EndMessage`.
//! - **The handler fails**: [`ExplicitRecoveryVM::notify_error`] writes `ErrorMessage`. So does
//!   every error the VM raises itself: a violation of the protocol by the runtime (571), a misuse
//!   of the VM by the SDK (500).
//! - **The runtime shuts it down**: it closes its side of the stream, and
//!   [`ExplicitRecoveryVM::notify_input_closed`] writes `ShutdownMessage` at once, after the
//!   actions the handler already made, with no user code in between. An open atomic group is
//!   dropped.
//! - **The SDK goes away**: [`ExplicitRecoveryVM::shutdown`] writes an unasked `ShutdownMessage`.
//!
//! After `EndMessage` or `ErrorMessage` the runtime closes its side first, and the SDK after it:
//! that is what [`ExplicitRecoveryVM::should_close_output`] says.
//!
//! # Invalid input
//!
//! A value user code passes to an action that the runtime would refuse as malformed fails that
//! action alone (D65): an invocation id to attach to, cancel or signal that is not one in
//! Restate's format, a scope outside `[a-zA-Z0-9_.-]{1,36}`, an empty idempotency key or limit
//! key, a limit key without a scope. The call returns an error with
//! [`crate::Error::is_invalid_input`] (code 400), which the SDK raises to user code as a terminal
//! error it can catch. The VM sends nothing and takes no result id for it, and the attempt goes
//! on. In an atomic group the group stays open: the SDK aborts it, as on every error in it.
//! Every other error of an action still ends the attempt. An action on the wire that the runtime
//! finds malformed anyway stays a protocol violation: an SDK bug, not bad input.
//!
//! # Atomic groups
//!
//! [`ExplicitRecoveryVM::atomic_group_start`] opens a group: state changes, calls, sends,
//! cancels, signal sends and spans made until [`ExplicitRecoveryVM::atomic_group_end`] are kept,
//! and leave as one `AtomicGroupActionMessage`. A read, an attach, a signal read, a flush, a
//! group or the end fail the attempt meanwhile. The group is one synchronous section: never await
//! between its start and its end, and call [`ExplicitRecoveryVM::atomic_group_abort`] on every
//! error path.

pub mod commit;
#[cfg(any(test, feature = "__fake_runtime"))]
pub mod fake_runtime;
mod user_input;
mod vm;

pub use crate::service_protocol::explicit_recovery::ProtocolVersion;
pub use crate::CANCEL_NOTIFICATION_HANDLE;
pub use vm::{missing_features, CoreExplicitRecoveryVM};

use super::{
    AttachInvocationTarget, CallHandle, Error, Header, HeaderMap, NonEmptyValue,
    NotificationHandle, SendHandle, Target, VMResult, Value,
};
use bytes::Bytes;
use std::num::NonZero;
use std::time::Duration;

/// The result of the action that returned `handle`, or the cancel signal
/// ([`crate::CANCEL_NOTIFICATION_HANDLE`]).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Notification {
    pub handle: NotificationHandle,
    pub value: Value,
}

/// A span started with [`ExplicitRecoveryVM::span_start`].
#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SpanId(pub(crate) NonZero<u32>);

impl From<SpanId> for u32 {
    fn from(value: SpanId) -> Self {
        value.0.get()
    }
}

impl TryFrom<u32> for SpanId {
    type Error = std::num::TryFromIntError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        NonZero::try_from(value).map(SpanId)
    }
}

/// Whose state a state key is in.
#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq)]
pub enum StateScope {
    /// The state of the object the invocation targets.
    Object,
    /// The state of the invocation: every attempt of it finds it, and it ends with it.
    Invocation,
}

/// How much of an action the runtime records.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum RecordLevel {
    /// What the runtime keeps for that kind of action.
    #[default]
    Default,
    None,
    Summary,
    Full,
}

/// How an action appears in the runtime's record of the attempt. None of it changes what the
/// action does.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ActionOptions {
    pub name: Option<String>,
    /// The span the action is inside of; `None`: directly inside the attempt.
    pub parent_span: Option<SpanId>,
    pub record: RecordLevel,
}

impl ActionOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Default::default()
        }
    }

    pub fn recorded(mut self, record: RecordLevel) -> Self {
        self.record = record;
        self
    }

    pub fn inside(mut self, span: SpanId) -> Self {
        self.parent_span = Some(span);
        self
    }
}

/// What [`ExplicitRecoveryVM::get_input`] returns: the invocation, and this attempt of it.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Input {
    pub invocation_id: String,
    pub key: String,
    pub headers: Vec<Header>,
    pub input: Bytes,
    pub scope: Option<String>,
    pub limit_key: Option<String>,
    pub idempotency_key: Option<String>,
    /// The number of this attempt: 1 for the first.
    pub attempt: u32,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ResponseHead {
    pub status_code: u16,
    pub headers: Vec<Header>,
    pub version: ProtocolVersion,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
    /// A value this attempt writes is kept, to answer its own reads without asking Restate, when
    /// it is at most this large. Of a larger one, only that the key has a value is kept.
    pub keep_written_values_up_to: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            keep_written_values_up_to: 4 * 1024,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AttemptState {
    /// Waiting for `StartMessage`.
    Starting,
    /// The handler runs.
    Running,
    /// The attempt is over: the handler ended, the attempt was shut down, or it failed.
    Closed,
}

/// State machine of an attempt of a handler on the explicit recovery protocol. See the module
/// documentation for how an SDK drives it.
pub trait ExplicitRecoveryVM: Sized {
    /// Accepts the content type of this family only, and of a version it speaks: otherwise an
    /// error with code 415.
    fn new(request_headers: impl HeaderMap, options: Options) -> VMResult<Self>;

    fn get_response_head(&self) -> ResponseHead;

    // --- Input and output

    fn notify_input(&mut self, buffer: Bytes);

    /// The runtime closed its side of the stream. While the handler runs, this is a shutdown: the
    /// VM writes `ShutdownMessage` at once, and the attempt is over.
    fn notify_input_closed(&mut self);

    fn take_output(&mut self) -> Bytes;

    /// Whether the SDK should now close its side of the stream: the VM wrote its last message
    /// (`EndMessage`, `ErrorMessage` or `ShutdownMessage`), and the runtime closed its side.
    /// Write [`ExplicitRecoveryVM::take_output`] first.
    fn should_close_output(&self) -> bool;

    // --- Start

    fn is_ready_to_execute(&self) -> VMResult<bool>;

    fn get_input(&mut self) -> VMResult<Input>;

    /// Whether the attempt supports the feature `feature`. In version 1 there is none.
    fn is_feature_supported(&self, feature: u32) -> bool;

    // --- Results

    /// The next notification, in the order the runtime sent them. Taking it is processing it:
    /// the SDK settles its promise before it yields.
    fn take_next_notification(&mut self) -> VMResult<Option<Notification>>;

    // --- State

    fn cmd_state_get(
        &mut self,
        scope: StateScope,
        key: String,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle>;

    fn cmd_state_get_keys(
        &mut self,
        scope: StateScope,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle>;

    /// `want_result`: ask for the result, `Void` once the write is applied. Without it, `None`.
    fn cmd_state_set(
        &mut self,
        scope: StateScope,
        key: String,
        value: Bytes,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    fn cmd_state_clear(
        &mut self,
        scope: StateScope,
        key: String,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    fn cmd_state_clear_all(
        &mut self,
        scope: StateScope,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    // --- Invocations

    /// `invoke_at`: when to make the invocation, as time since the UNIX epoch. An empty
    /// idempotency key or limit key, a limit key without a scope, or a scope that is not one: an
    /// invalid input error (see the module documentation), as for [`ExplicitRecoveryVM::cmd_send`].
    fn cmd_call(
        &mut self,
        target: Target,
        input: Bytes,
        invoke_at: Option<Duration>,
        options: ActionOptions,
    ) -> VMResult<CallHandle>;

    /// A call nobody waits for: a `Call` without a result id.
    fn cmd_send(
        &mut self,
        target: Target,
        input: Bytes,
        invoke_at: Option<Duration>,
        options: ActionOptions,
    ) -> VMResult<SendHandle>;

    /// An invocation id that is not one, or a scope that is not one: an invalid input error (see
    /// the module documentation).
    fn cmd_attach_invocation(
        &mut self,
        target: AttachInvocationTarget,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle>;

    /// An invocation id that is not one: an invalid input error (see the module documentation).
    fn cmd_cancel_invocation(
        &mut self,
        invocation_id: String,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    // --- Signals

    /// The signal `index` (from 0) of the name `name` that this invocation received.
    fn cmd_signal_read(
        &mut self,
        name: String,
        index: u32,
        options: ActionOptions,
    ) -> VMResult<NotificationHandle>;

    /// `value`: `None` for a signal that carries nothing. An invocation id that is not one: an
    /// invalid input error (see the module documentation).
    fn cmd_signal_send(
        &mut self,
        target_invocation_id: String,
        name: String,
        value: Option<NonEmptyValue>,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    // --- Durability

    /// Completes with `Void` once every action made before it is applied.
    fn cmd_flush(&mut self, options: ActionOptions) -> VMResult<NotificationHandle>;

    /// Opens an atomic group: the actions made until [`ExplicitRecoveryVM::atomic_group_end`] are
    /// kept, and leave as one message. Not across an await: the VM is modal meanwhile.
    fn atomic_group_start(&mut self) -> VMResult<()>;

    /// Closes the group and sends it. `want_result`: its own result, `Void` once it is applied.
    fn atomic_group_end(
        &mut self,
        options: ActionOptions,
        want_result: bool,
    ) -> VMResult<Option<NotificationHandle>>;

    /// Drops the open group and the actions made in it. Returns the handles those returned,
    /// which will never be notified.
    fn atomic_group_abort(&mut self) -> VMResult<Vec<NotificationHandle>>;

    // --- Spans

    fn span_start(&mut self, name: String, parent: Option<SpanId>) -> VMResult<SpanId>;

    fn span_end(&mut self, span: SpanId, result: Option<NonEmptyValue>) -> VMResult<()>;

    // --- Control

    /// `HeartbeatMessage`: the handler works on something the runtime does not see. Send one
    /// more often than the inactivity timeout while not waiting on a result. Also while an atomic
    /// group is open: it is not an action.
    fn heartbeat(&mut self) -> VMResult<()>;

    // --- End

    /// Ends the invocation with its output: `EndMessage`. Fails while an atomic group is open.
    fn end(&mut self, output: NonEmptyValue) -> VMResult<()>;

    /// Ends the attempt with `ErrorMessage`, which carries the error's metadata. Nothing, once
    /// the attempt is over.
    fn notify_error(&mut self, error: Error);

    /// The SDK ends the attempt on its own, for example because its process is going away: an
    /// unasked `ShutdownMessage`, after the actions already made. The runtime restarts the
    /// invocation, with no retry counted. An open atomic group is dropped. Nothing, once the
    /// attempt is over.
    fn shutdown(&mut self);

    /// Whether the invocation is cancelled, as far as the handler was told: it was before the
    /// attempt, or the cancel signal was taken with [`ExplicitRecoveryVM::take_next_notification`].
    fn is_cancelled(&self) -> bool;

    fn state(&self) -> AttemptState;
}
