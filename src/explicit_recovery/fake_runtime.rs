//! A fake Restate runtime for the explicit recovery protocol, version 1: one invocation, over as
//! many attempts as it takes.
//!
//! **Internal, for tests** (feature `__fake_runtime`, and the crate's own tests): no API stability.
//!
//! It does no I/O, as the VM: bytes in ([`FakeRuntime::receive`]), bytes out
//! ([`FakeRuntime::take_output`]). It plays the runtime's side of the specification ("the
//! spec", `10-protocol-spec.md`):
//!
//! - it applies an attempt's actions in the order sent (G1), one by one, a group as one step (G3),
//!   and sends each result only once its action and every one before it is applied (G8). With
//!   [`FakeRuntime::set_auto_apply`] off, what it received waits until
//!   [`FakeRuntime::apply_pending`]: the lag of G8;
//! - it keeps the invocation across attempts: its state of both scopes, its signals, whether it
//!   was cancelled, the next result id; and a new attempt starts only once the earlier one is
//!   over (G4);
//! - after a shutdown it decides between restart and suspend with the six rules of spec 10.5, and
//!   bounds idle restarts (three in a row, then a failure). A suspended invocation wakes when
//!   anything arrives for it (spec 10.6);
//! - after `EndMessage` or `ErrorMessage` it closes its side first ([`FakeRuntime::is_closed`]),
//!   and the SDK closes after it ([`FakeRuntime::sdk_closed`]);
//! - it checks what the SDK sends against spec 12: every violation it finds fails the attempt, is
//!   returned by [`FakeRuntime::receive`], and is kept in [`FakeRuntime::violations`]. So it is
//!   also a linter for SDKs.
//!
//! The rest of the world is scripted by the test: what callees do ([`FakeRuntime::script_callee`],
//! [`FakeRuntime::complete_call`]), signals that arrive ([`FakeRuntime::send_signal`]), the cancel
//! signal ([`FakeRuntime::cancel`]), shutdowns ([`FakeRuntime::shut_down`]), lost attempts and
//! broken streams.
//!
//! # Driving it with the VM
//!
//! ```text
//! let mut runtime = FakeRuntime::new(InvocationSpec::default());
//! let mut vm = CoreExplicitRecoveryVM::new(headers_of(runtime.content_type()), Options::default())?;
//! vm.notify_input(runtime.start_attempt()?);
//! loop {
//!     // the handler runs, and calls the VM
//!     runtime.receive(vm.take_output())?;
//!     vm.notify_input(runtime.take_output());
//!     if runtime.is_closed() { vm.notify_input_closed(); }
//!     // take the VM's notifications, settle the handler's promises
//! }
//! // once vm.should_close_output(): runtime.receive(vm.take_output())?; runtime.sdk_closed();
//! ```

use crate::explicit_recovery::ProtocolVersion;
use crate::service_protocol::explicit_recovery::messages::{
    self, atomic_group_action_message::action::Action as Grouped,
    attach_invocation_action_message::Target as AttachTarget, end_message,
    result_notification_message::Result as ResultVariant, send_signal_action_message,
    start_message::StateEntry, AtomicGroupActionMessage, AttachInvocationActionMessage,
    BuiltInSignal, CallActionMessage, CancelInvocationActionMessage, ClearAllStateActionMessage,
    ClearStateActionMessage, EndMessage, ErrorBehavior, ErrorMessage, FlushActionMessage,
    GetStateActionMessage, GetStateKeysActionMessage, ReadSignalActionMessage, RestateMessage,
    ResultNotificationMessage, SendSignalActionMessage, SetStateActionMessage, ShutdownMessage,
    SignalNotificationMessage, SpanEndActionMessage, SpanStartActionMessage, StartMessage,
    StateKeys,
};
use crate::service_protocol::explicit_recovery::{Decoder, MessageType, RawMessage};
use crate::{TerminalFailure, Value};
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;

// --- What the test configures

/// How the runtime preloads one state scope into `StartMessage` (spec 8, 13.5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Preload {
    /// Every key of the scope: the scope is not partial.
    #[default]
    All,
    /// Nothing: the scope is partial, and the SDK asks for every key.
    Lazy,
    /// These keys, with their value or as known to have none: the scope is partial.
    Eager(Vec<String>),
}

/// The invocation the fake runtime runs.
#[derive(Debug, Clone)]
pub struct InvocationSpec {
    /// Its id, as `StartMessage.debug_id` and as the target of actions.
    pub invocation_id: String,
    pub service: String,
    pub handler: String,
    /// The object's key. `None`: a target without a key, which has no object state.
    pub key: Option<String>,
    /// A shared handler of a virtual object: it reads object state, and must not write it.
    pub shared: bool,
    pub input: Bytes,
    pub headers: Vec<(String, String)>,
    pub idempotency_key: Option<String>,
    pub scope: Option<String>,
    pub limit_key: Option<String>,
    /// The preload policy of object state.
    pub object_preload: Preload,
    /// The preload policy of invocation state.
    pub invocation_preload: Preload,
}

impl Default for InvocationSpec {
    fn default() -> Self {
        Self {
            invocation_id: "inv_1fakeruntime0000000000000000".to_owned(),
            service: "FakeObject".to_owned(),
            handler: "handle".to_owned(),
            key: Some("key".to_owned()),
            shared: false,
            input: Bytes::new(),
            headers: vec![],
            idempotency_key: None,
            scope: None,
            limit_key: None,
            object_preload: Preload::All,
            invocation_preload: Preload::All,
        }
    }
}

/// How the fake runtime behaves where the spec leaves it to the runtime.
#[derive(Debug, Clone)]
pub struct FakeRuntimeOptions {
    /// `StartMessage.next_result_id` of the first attempt. At least 1.
    pub first_next_result_id: u32,
    /// The largest payload of a message it accepts, in bytes. Above it: a violation.
    pub max_message_size: usize,
    /// After this many consecutive failed attempts the invocation is paused. `None`: never.
    pub max_consecutive_failures: Option<u32>,
    /// After this many idle restarts in a row, every further one is a failure (spec 10.5).
    pub max_idle_restarts: u32,
    /// `StartMessage.features`.
    pub features: Vec<u32>,
}

impl Default for FakeRuntimeOptions {
    fn default() -> Self {
        Self {
            first_next_result_id: 1,
            max_message_size: 32 * 1024 * 1024,
            max_consecutive_failures: None,
            max_idle_restarts: 3,
            features: vec![],
        }
    }
}

/// What a callee does when a `Call` reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CalleeBehavior {
    /// It runs until the test completes it with [`FakeRuntime::complete_call`].
    Pending,
    /// It returns at once: `Value::Success`, `Value::Failure` (or `Value::Void`, an empty output).
    Returns(Value),
    /// The target cannot be invoked (no such service or handler): a failed invocation, with
    /// code 404.
    NotFound,
}

/// Why the runtime shuts an attempt down (spec 10.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    /// The attempt was silent for the whole inactivity timeout.
    Inactivity,
    /// A reason of the runtime's own: scheduling, a drain.
    Own,
}

// --- What the test observes

/// Where the invocation stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationStatus {
    /// A new attempt can start: the first, a restart, a retry or a wake-up.
    Ready,
    /// An attempt runs.
    Running,
    /// Suspended until something arrives for it.
    Suspended,
    /// Paused, until [`FakeRuntime::resume`] or the cancel signal.
    Paused,
    /// Completed, with its output: `Value::Success` or `Value::Failure`.
    Completed(Value),
}

/// How an attempt ended, as the runtime sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// `EndMessage`: the invocation completed with its output.
    Completed,
    /// `ErrorMessage` with behavior `FAIL`: the invocation failed with it.
    FailedInvocation { code: u32 },
    /// `ErrorMessage` with behavior `PAUSE`.
    Paused { code: u32 },
    /// The attempt failed, and the retry policy applies: `ErrorMessage` with `RETRY`, a broken
    /// stream, a violation, an idle restart beyond the bound.
    Failed { reason: String },
    /// Restarted at once, by this rule of spec 10.5 (1 to 5). No retry is counted.
    Restarted { rule: u8 },
    /// The runtime lost the attempt (a failover): restarted, no retry counted.
    Lost,
    /// Suspended (rule 6 of spec 10.5).
    Suspended,
}

/// An invocation a `Call` made, and what the fake knows of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRecord {
    pub invocation_id: String,
    pub service: String,
    pub handler: String,
    pub key: String,
    pub idempotency_key: Option<String>,
    pub parameter: Bytes,
    /// Milliseconds since the UNIX epoch; 0 for as soon as possible.
    pub invoke_time: u64,
    /// The attempt that made it.
    pub attempt: u32,
    /// What it returned, once it did.
    pub output: Option<Value>,
}

/// A signal an action sent to another invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentSignal {
    pub target_invocation_id: String,
    pub name: String,
    /// `Value::Void`, `Value::Success` or `Value::Failure`.
    pub value: Value,
}

/// A misuse of the fake by the test, or a violation of the protocol by the SDK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeRuntimeError {
    /// The SDK violated the protocol (spec 12). The attempt failed.
    Violation(String),
    /// The test asked for something the fake cannot do now.
    Misuse(String),
}

impl fmt::Display for FakeRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FakeRuntimeError::Violation(v) => write!(f, "protocol violation by the SDK: {v}"),
            FakeRuntimeError::Misuse(m) => write!(f, "fake runtime misuse: {m}"),
        }
    }
}

impl std::error::Error for FakeRuntimeError {}

type FakeResult<T> = Result<T, FakeRuntimeError>;

fn misuse<T>(what: impl Into<String>) -> FakeResult<T> {
    Err(FakeRuntimeError::Misuse(what.into()))
}

// --- Internals

/// An external result some attempt waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WaitOn {
    /// What the invocation returns: a call's result, or an attach.
    Invocation(String),
    Signal(String, u32),
}

#[derive(Debug, Clone)]
struct Wait {
    attempt: u32,
    result_id: u32,
    on: WaitOn,
}

/// How the runtime's side of the stream got closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Closed {
    /// A shutdown.
    Shutdown(ShutdownReason),
    /// The SDK's last message came: `EndMessage`, `ErrorMessage`, or an unasked `ShutdownMessage`.
    AfterLastMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LastMessage {
    End,
    Error,
    Shutdown,
}

struct Attempt {
    number: u32,
    decoder: Decoder,
    /// Received, not applied yet.
    received: VecDeque<RawMessage>,
    /// The notifications to send.
    out: Vec<Bytes>,
    /// Notifications sent in this attempt: results and the cancel signal.
    sent_notifications: u32,
    /// Notifications that came for the attempt after the runtime closed its side, and could no
    /// longer be sent (rule 3).
    arrived_after_close: u32,
    /// The highest result id used so far; `next_result_id - 1` at the start.
    highest_result_id: u32,
    /// Result ids this attempt sent, whose result was not sent yet: G7, at most one result each.
    open_result_ids: HashSet<u32>,
    spans: HashSet<u32>,
    actions_applied: u32,
    closed: Option<Closed>,
    /// The SDK sent something other than its last message after the runtime closed (rule 4).
    sent_after_close: bool,
    last_message: Option<LastMessage>,
    /// The attempt knows of the cancellation: from its start, or from the notification.
    knows_cancel: bool,
    /// The attempt is over: its outcome is decided.
    over: bool,
    sdk_closed: bool,
}

// --- Bound checks: a binding can hold it in a class of its own.
#[allow(unused)]
const fn is_send<T: Send>() {}
const _: () = is_send::<FakeRuntime>();

/// The fake runtime. See the module documentation.
pub struct FakeRuntime {
    spec: InvocationSpec,
    options: FakeRuntimeOptions,
    status: InvocationStatus,

    object_state: BTreeMap<String, Bytes>,
    invocation_state: BTreeMap<String, Bytes>,
    signals: BTreeMap<String, Vec<Value>>,
    cancelled: bool,
    next_result_id: u32,
    attempt_number: u32,

    outcomes: Vec<AttemptOutcome>,
    consecutive_failures: u32,
    idle_restarts_in_a_row: u32,
    last_retry_delay: Option<u64>,

    callee_behaviors: HashMap<(String, String), CalleeBehavior>,
    calls: Vec<CallRecord>,
    signals_sent: Vec<SentSignal>,
    cancels_sent: Vec<String>,
    waits: Vec<Wait>,
    violations: Vec<String>,

    auto_apply: bool,
    attempt: Option<Attempt>,
}

impl FakeRuntime {
    pub fn new(spec: InvocationSpec) -> Self {
        Self::with_options(spec, FakeRuntimeOptions::default())
    }

    pub fn with_options(spec: InvocationSpec, options: FakeRuntimeOptions) -> Self {
        assert!(
            options.first_next_result_id >= 1,
            "next_result_id is at least 1"
        );
        Self {
            next_result_id: options.first_next_result_id,
            spec,
            options,
            status: InvocationStatus::Ready,
            object_state: Default::default(),
            invocation_state: Default::default(),
            signals: Default::default(),
            cancelled: false,
            attempt_number: 0,
            outcomes: vec![],
            consecutive_failures: 0,
            idle_restarts_in_a_row: 0,
            last_retry_delay: None,
            callee_behaviors: Default::default(),
            calls: vec![],
            signals_sent: vec![],
            cancels_sent: vec![],
            waits: vec![],
            violations: vec![],
            auto_apply: true,
            attempt: None,
        }
    }

    /// The `content-type` of the request that opens an attempt.
    pub fn content_type(&self) -> &'static str {
        ProtocolVersion::V1.content_type()
    }

    // --- Setting the scene

    /// Object state, as it is before the first attempt.
    pub fn object_state_mut(&mut self) -> &mut BTreeMap<String, Bytes> {
        &mut self.object_state
    }

    /// Invocation state, as it is before the first attempt.
    pub fn invocation_state_mut(&mut self) -> &mut BTreeMap<String, Bytes> {
        &mut self.invocation_state
    }

    /// What a `Call` to `service`/`handler` does. Unscripted callees are [`CalleeBehavior::Pending`].
    pub fn script_callee(
        &mut self,
        service: impl Into<String>,
        handler: impl Into<String>,
        behavior: CalleeBehavior,
    ) {
        self.callee_behaviors
            .insert((service.into(), handler.into()), behavior);
    }

    // --- Observing

    pub fn status(&self) -> &InvocationStatus {
        &self.status
    }

    pub fn object_state(&self) -> &BTreeMap<String, Bytes> {
        &self.object_state
    }

    pub fn invocation_state(&self) -> &BTreeMap<String, Bytes> {
        &self.invocation_state
    }

    /// The outcome of every attempt that ended, in order.
    pub fn outcomes(&self) -> &[AttemptOutcome] {
        &self.outcomes
    }

    /// The number of the last attempt started: 0 before the first.
    pub fn attempt_number(&self) -> u32 {
        self.attempt_number
    }

    /// `next_result_id` of the next attempt.
    pub fn next_result_id(&self) -> u32 {
        self.next_result_id
    }

    /// Every invocation a `Call` made, sends included, in order.
    pub fn calls(&self) -> &[CallRecord] {
        &self.calls
    }

    pub fn signals_sent(&self) -> &[SentSignal] {
        &self.signals_sent
    }

    /// The invocation ids `CancelInvocation` named.
    pub fn cancels_sent(&self) -> &[String] {
        &self.cancels_sent
    }

    /// Every violation of the protocol by the SDK, across attempts.
    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    /// `ErrorMessage.next_retry_delay` of the last attempt that failed with one.
    pub fn last_retry_delay(&self) -> Option<u64> {
        self.last_retry_delay
    }

    /// Whether the runtime has closed its side of the running attempt's stream: after a shutdown,
    /// or after the SDK's last message. The harness then tells the VM
    /// (`notify_input_closed`), after it delivered [`FakeRuntime::take_output`].
    pub fn is_closed(&self) -> bool {
        self.attempt
            .as_ref()
            .is_some_and(|attempt| attempt.closed.is_some() || attempt.over)
    }

    /// Whether an attempt runs and is not over.
    pub fn is_attempt_running(&self) -> bool {
        self.attempt.as_ref().is_some_and(|attempt| !attempt.over)
    }

    // --- The attempt

    /// Start an attempt: returns its `StartMessage`. The invocation must be
    /// [`InvocationStatus::Ready`], and the earlier attempt over (G4).
    pub fn start_attempt(&mut self) -> FakeResult<Bytes> {
        if self.status != InvocationStatus::Ready {
            return misuse(format!(
                "no attempt can start: the invocation is {:?}",
                self.status
            ));
        }
        if self.is_attempt_running() {
            return misuse("an attempt runs");
        }
        self.attempt_number += 1;
        let number = self.attempt_number;
        self.status = InvocationStatus::Running;

        let object_partial = self.spec.key.is_none() || self.spec.object_preload != Preload::All;
        let mut state_map = vec![];
        if self.spec.key.is_some() {
            preload(
                &mut state_map,
                messages::StateScope::Object,
                &self.object_state,
                &self.spec.object_preload,
            );
        }
        preload(
            &mut state_map,
            messages::StateScope::Invocation,
            &self.invocation_state,
            &self.spec.invocation_preload,
        );

        let start = StartMessage {
            id: Bytes::from(self.spec.invocation_id.clone().into_bytes()),
            debug_id: self.spec.invocation_id.clone(),
            key: self.spec.key.clone().unwrap_or_default(),
            headers: self
                .spec
                .headers
                .iter()
                .map(|(key, value)| messages::Header {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            input: Some(self.spec.input.clone().into()),
            scope: self.spec.scope.clone(),
            limit_key: self.spec.limit_key.clone(),
            idempotency_key: self.spec.idempotency_key.clone(),
            attempt: number,
            next_result_id: self.next_result_id,
            cancelled: self.cancelled,
            state_map,
            is_object_scope_partial_state: object_partial,
            is_invocation_scope_partial_state: self.spec.invocation_preload != Preload::All,
            features: self.options.features.clone(),
        };
        self.attempt = Some(Attempt {
            number,
            decoder: Decoder::for_registry(),
            received: Default::default(),
            out: vec![],
            sent_notifications: 0,
            arrived_after_close: 0,
            highest_result_id: self.next_result_id - 1,
            open_result_ids: Default::default(),
            spans: Default::default(),
            actions_applied: 0,
            closed: None,
            sent_after_close: false,
            last_message: None,
            knows_cancel: self.cancelled,
            over: false,
            sdk_closed: false,
        });
        Ok(messages::encode(&start))
    }

    /// What the SDK wrote. Decoded, and applied unless auto-apply is off.
    ///
    /// Returns the first violation it finds: the attempt then failed. What comes after the
    /// SDK's last message is dropped, and reported as a violation, but the last message stands.
    pub fn receive(&mut self, bytes: Bytes) -> FakeResult<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let max_message_size = self.options.max_message_size;
        let Some(attempt) = self.attempt.as_mut() else {
            return misuse("no attempt to receive bytes for");
        };
        if attempt.sdk_closed {
            return misuse("the SDK closed its side already");
        }
        if attempt.over {
            if attempt.last_message.is_some() {
                return self.violation_after_last_message();
            }
            // The stream was closed by a violation or a lost attempt: nothing is read anymore.
            return Ok(());
        }
        attempt.decoder.push(bytes);
        loop {
            let attempt = self.attempt.as_mut().unwrap();
            let raw = match attempt.decoder.consume_next() {
                Ok(Some(raw)) => raw,
                Ok(None) => break,
                Err(e) => return self.violation(format!("cannot decode a message: {e}")),
            };
            let header = raw.raw_header();
            if header.flags() != 0 {
                return self.violation(format!(
                    "the reserved bits of the header of {:?} are set",
                    header.ty()
                ));
            }
            if header.length() as usize > max_message_size {
                return self.violation(format!(
                    "{:?} is {} bytes, above the limit of {max_message_size}",
                    header.ty(),
                    header.length()
                ));
            }
            let ty = header.ty();
            if !(ty.is_action() || ty == MessageType::Heartbeat || is_last_message(ty)) {
                return self.violation(format!("{ty:?} is not a message the SDK sends"));
            }
            if attempt.last_message.is_some() {
                return self.violation_after_last_message();
            }
            if attempt.closed.is_some() && !is_last_message(ty) {
                // Rule 4: a message that crossed the shutdown on the wire. Still applied.
                attempt.sent_after_close = true;
            }
            if is_last_message(ty) {
                attempt.last_message = Some(match ty {
                    MessageType::End => LastMessage::End,
                    MessageType::Error => LastMessage::Error,
                    _ => LastMessage::Shutdown,
                });
                // After the SDK's last message the runtime closes its side at once (D35).
                if attempt.closed.is_none() {
                    attempt.closed = Some(Closed::AfterLastMessage);
                }
            }
            attempt.received.push_back(raw);
            if self.auto_apply {
                // Applied as read: a violation further on leaves what came before applied (G2).
                self.apply_pending(usize::MAX)?;
            }
        }
        Ok(())
    }

    /// Notifications for the SDK, in the order sent.
    pub fn take_output(&mut self) -> Bytes {
        let Some(attempt) = self.attempt.as_mut() else {
            return Bytes::new();
        };
        let mut buf = BytesMut::new();
        for b in attempt.out.drain(..) {
            buf.extend_from_slice(&b);
        }
        buf.freeze()
    }

    /// Off: what [`FakeRuntime::receive`] gets waits for [`FakeRuntime::apply_pending`]. The
    /// attempt is not silent meanwhile: the runtime owes it something.
    pub fn set_auto_apply(&mut self, auto_apply: bool) {
        self.auto_apply = auto_apply;
    }

    /// Apply up to `max` of the messages received and not applied yet, in order. Returns how
    /// many it applied.
    pub fn apply_pending(&mut self, max: usize) -> FakeResult<usize> {
        let mut applied = 0;
        while applied < max {
            let Some(attempt) = self.attempt.as_mut() else {
                break;
            };
            if attempt.over {
                break;
            }
            let Some(raw) = attempt.received.pop_front() else {
                break;
            };
            self.apply(raw)?;
            applied += 1;
        }
        Ok(applied)
    }

    /// The runtime shuts the attempt down: it closes its side of the stream, and sends nothing
    /// more. The SDK answers with `ShutdownMessage` ([`FakeRuntime::receive`]), or not
    /// ([`FakeRuntime::shutdown_answer_timed_out`], [`FakeRuntime::sdk_closed`]).
    ///
    /// For [`ShutdownReason::Inactivity`] the attempt must be silent: no message received and
    /// not applied.
    pub fn shut_down(&mut self, reason: ShutdownReason) -> FakeResult<()> {
        let Some(attempt) = self.attempt.as_mut().filter(|attempt| !attempt.over) else {
            return misuse("no attempt runs");
        };
        if attempt.closed.is_some() {
            return misuse("the runtime closed its side already");
        }
        if reason == ShutdownReason::Inactivity && !attempt.received.is_empty() {
            return misuse(
                "the attempt is not silent: the runtime holds actions it has not applied",
            );
        }
        attempt.closed = Some(Closed::Shutdown(reason));
        Ok(())
    }

    /// The SDK did not send its last message in time after a shutdown: rule 2.
    pub fn shutdown_answer_timed_out(&mut self) -> FakeResult<()> {
        let Some(attempt) = self.attempt.as_mut().filter(|attempt| !attempt.over) else {
            return misuse("no attempt runs");
        };
        if !matches!(attempt.closed, Some(Closed::Shutdown(_))) {
            return misuse("the runtime did not shut the attempt down");
        }
        self.apply_pending(usize::MAX)?;
        if self.is_attempt_running() {
            self.end_without_last_message();
        }
        Ok(())
    }

    /// The SDK closed its side of the stream. After its last message, as it should (D35); before
    /// it, the attempt ends without one: a failure if the runtime had not closed its side, rule 2
    /// if it had.
    pub fn sdk_closed(&mut self) -> FakeResult<()> {
        let Some(attempt) = self.attempt.as_mut() else {
            return misuse("no attempt");
        };
        if attempt.sdk_closed {
            return misuse("the SDK closed its side already");
        }
        attempt.sdk_closed = true;
        if !attempt.over {
            // What is in the pipe is applied first (G2: a prefix, here all of it).
            self.apply_pending(usize::MAX)?;
            if self.is_attempt_running() {
                self.end_without_last_message();
            }
        }
        Ok(())
    }

    /// The stream breaks. Like [`FakeRuntime::sdk_closed`], but nothing more is read: the
    /// messages received and not applied are lost (G2 keeps a prefix).
    pub fn break_stream(&mut self) -> FakeResult<()> {
        let Some(attempt) = self.attempt.as_mut().filter(|attempt| !attempt.over) else {
            return misuse("no attempt runs");
        };
        attempt.received.clear();
        attempt.sdk_closed = true;
        self.end_without_last_message();
        Ok(())
    }

    /// The runtime loses the attempt (a failover, a crash): the messages received and not
    /// applied are lost (G2), and a new attempt starts, with no retry counted.
    pub fn lose_attempt(&mut self) -> FakeResult<()> {
        let Some(attempt) = self.attempt.as_mut().filter(|attempt| !attempt.over) else {
            return misuse("no attempt runs");
        };
        attempt.received.clear();
        self.finish(AttemptOutcome::Lost);
        Ok(())
    }

    // --- The world

    /// The invocation `invocation_id`, which a `Call` made, returns `output`.
    pub fn complete_call(&mut self, invocation_id: &str, output: Value) -> FakeResult<()> {
        let Some(call) = self
            .calls
            .iter_mut()
            .find(|call| call.invocation_id == invocation_id)
        else {
            return misuse(format!("no call made the invocation {invocation_id}"));
        };
        if call.output.is_some() {
            return misuse(format!("{invocation_id} already returned"));
        }
        call.output = Some(output.clone());
        self.arrived(WaitOn::Invocation(invocation_id.to_owned()), output);
        Ok(())
    }

    /// A signal named `name` arrives for the invocation: `Value::Void`, `Value::Success` or
    /// `Value::Failure`. It gets the next index of its name. A signal after the end is dropped.
    pub fn send_signal(&mut self, name: &str, value: Value) {
        if matches!(self.status, InvocationStatus::Completed(_)) {
            return;
        }
        let signals = self.signals.entry(name.to_owned()).or_default();
        let index = signals.len() as u32;
        signals.push(value.clone());
        self.arrived(WaitOn::Signal(name.to_owned(), index), value);
        // Any signal wakes a suspended invocation, whether or not someone reads it.
        self.wake();
    }

    /// The cancel signal arrives for the invocation (spec 11).
    pub fn cancel(&mut self) {
        if self.cancelled || matches!(self.status, InvocationStatus::Completed(_)) {
            // A second cancel signal changes nothing, and wakes nothing.
            return;
        }
        self.cancelled = true;
        if self.attempt_number == 0 {
            // An invocation cancelled before its first attempt never starts.
            self.status = InvocationStatus::Completed(Value::Failure(cancelled_failure()));
            return;
        }
        if let Some(attempt) = self.attempt.as_mut().filter(|attempt| !attempt.over) {
            if !attempt.knows_cancel {
                attempt.knows_cancel = true;
                if attempt.closed.is_some() {
                    attempt.arrived_after_close += 1;
                } else {
                    attempt.sent_notifications += 1;
                    attempt
                        .out
                        .push(messages::encode(&SignalNotificationMessage {
                            signal_id: BuiltInSignal::Cancel as i32,
                        }));
                }
            }
        }
        if self.status == InvocationStatus::Paused {
            self.status = InvocationStatus::Ready;
        }
        self.wake();
    }

    /// An operator resumes a paused or suspended invocation.
    pub fn resume(&mut self) -> FakeResult<()> {
        match self.status {
            InvocationStatus::Paused | InvocationStatus::Suspended => {
                self.status = InvocationStatus::Ready;
                Ok(())
            }
            _ => misuse(format!("cannot resume: {:?}", self.status)),
        }
    }

    // --- Internals: ends

    fn violation(&mut self, what: String) -> FakeResult<()> {
        self.violations.push(what.clone());
        if self.is_attempt_running() {
            self.fail(format!("protocol violation: {what}"));
        }
        Err(FakeRuntimeError::Violation(what))
    }

    fn violation_after_last_message(&mut self) -> FakeResult<()> {
        // The runtime drops what came after the last message; what that said stands.
        let what = "a message after the SDK's last message".to_owned();
        self.violations.push(what.clone());
        Err(FakeRuntimeError::Violation(what))
    }

    fn end_without_last_message(&mut self) {
        let attempt = self.attempt.as_ref().unwrap();
        match attempt.closed {
            // After the runtime closed its side, every end without a last message is rule 2.
            Some(_) => self.restart_or_suspend(None),
            None => self.fail("the stream ended without a last message".to_owned()),
        }
    }

    fn fail(&mut self, reason: String) {
        self.finish(AttemptOutcome::Failed { reason });
    }

    /// Spec 10.5, in order. `answer`: the count of `ShutdownMessage`, if it came.
    fn restart_or_suspend(&mut self, answer: Option<u32>) {
        let attempt = self.attempt.as_ref().unwrap();
        let had = attempt.sent_notifications + attempt.arrived_after_close;
        let number = attempt.number;
        let rule = match (attempt.closed, answer) {
            (Some(Closed::Shutdown(ShutdownReason::Own)), _)
            | (Some(Closed::AfterLastMessage), _) => Some(1),
            (_, None) => Some(2),
            (_, Some(processed)) if processed < had => Some(3),
            _ if attempt.sent_after_close => Some(4),
            _ if !self.waits.iter().any(|wait| wait.attempt == number) => Some(5),
            _ => None,
        };
        match rule {
            Some(rule) => self.finish(AttemptOutcome::Restarted { rule }),
            None => self.finish(AttemptOutcome::Suspended),
        }
    }

    /// The attempt is over, with `outcome`: what follows for the invocation.
    fn finish(&mut self, mut outcome: AttemptOutcome) {
        let attempt = self.attempt.as_mut().unwrap();
        attempt.over = true;
        attempt.received.clear();
        // The runtime closes its side: nothing more is sent.
        if attempt.closed.is_none() {
            attempt.closed = Some(Closed::AfterLastMessage);
        }
        let had_action_applied = attempt.actions_applied > 0;
        // Result ids go on across attempts, above every id this one used.
        self.next_result_id = attempt.highest_result_id + 1;

        // Idle restarts (spec 10.5): rules 2 to 5, with no action applied.
        if had_action_applied {
            self.idle_restarts_in_a_row = 0;
        } else if let AttemptOutcome::Restarted { rule: 2..=5 } = outcome {
            if self.idle_restarts_in_a_row >= self.options.max_idle_restarts {
                outcome = AttemptOutcome::Failed {
                    reason: format!(
                        "an idle restart after {} in a row",
                        self.idle_restarts_in_a_row
                    ),
                };
            } else {
                self.idle_restarts_in_a_row += 1;
            }
        }

        let is_failure = matches!(outcome, AttemptOutcome::Failed { .. });
        self.consecutive_failures = if is_failure {
            self.consecutive_failures + 1
        } else {
            0
        };
        self.status = match &outcome {
            AttemptOutcome::Completed | AttemptOutcome::FailedInvocation { .. } => {
                self.status.clone()
            }
            AttemptOutcome::Paused { .. } => InvocationStatus::Paused,
            AttemptOutcome::Failed { .. } => match self.options.max_consecutive_failures {
                Some(max) if self.consecutive_failures >= max => InvocationStatus::Paused,
                _ => InvocationStatus::Ready,
            },
            AttemptOutcome::Restarted { .. } | AttemptOutcome::Lost => InvocationStatus::Ready,
            AttemptOutcome::Suspended => InvocationStatus::Suspended,
        };
        if matches!(self.status, InvocationStatus::Completed(_)) {
            self.waits.clear();
        }
        self.outcomes.push(outcome);
    }

    fn complete(&mut self, output: Value, outcome: AttemptOutcome) {
        self.status = InvocationStatus::Completed(output);
        self.finish(outcome);
    }

    /// Something arrived for a suspended invocation: a new attempt (spec 10.6).
    fn wake(&mut self) {
        if self.status == InvocationStatus::Suspended {
            self.status = InvocationStatus::Ready;
        }
    }

    /// An external result: to the running attempt that waits for it, or dropped. Either way a
    /// suspended invocation whose attempt sent the action wakes.
    fn arrived(&mut self, on: WaitOn, value: Value) {
        let (waiting, rest): (Vec<_>, Vec<_>) =
            self.waits.drain(..).partition(|wait| wait.on == on);
        self.waits = rest;
        if waiting.is_empty() {
            return;
        }
        let running = self
            .attempt
            .as_ref()
            .filter(|attempt| !attempt.over)
            .map(|attempt| attempt.number);
        for wait in waiting {
            if Some(wait.attempt) == running {
                self.result(wait.result_id, result_variant(value.clone()));
            }
            // A result for an earlier attempt is dropped: the next attempt asks again.
        }
        self.wake();
    }

    // --- Internals: applying

    fn apply(&mut self, raw: RawMessage) -> FakeResult<()> {
        let ty = raw.raw_header().ty();
        let payload = raw.into_payload();
        macro_rules! decode {
            ($m:ty) => {
                match <$m as prost::Message>::decode(payload) {
                    Ok(m) => m,
                    Err(e) => {
                        return self.violation(format!(
                            "cannot decode {:?}: {e}",
                            <$m as RestateMessage>::ty()
                        ))
                    }
                }
            };
        }
        match ty {
            MessageType::Heartbeat => return Ok(()),
            MessageType::End => {
                let end = decode!(EndMessage);
                let Some(output) = end.output else {
                    return self.violation("EndMessage without an output".to_owned());
                };
                let output = match output {
                    end_message::Output::Value(value) => Value::Success(value.content),
                    end_message::Output::Failure(failure) => Value::Failure(failure.into()),
                };
                self.complete(output, AttemptOutcome::Completed);
                return Ok(());
            }
            MessageType::Error => {
                let error = decode!(ErrorMessage);
                self.last_retry_delay = error.next_retry_delay;
                match ErrorBehavior::try_from(error.behavior).unwrap_or(ErrorBehavior::Retry) {
                    ErrorBehavior::Retry => {
                        self.fail(format!("ErrorMessage {}: {}", error.code, error.message))
                    }
                    ErrorBehavior::Pause => {
                        self.finish(AttemptOutcome::Paused { code: error.code })
                    }
                    ErrorBehavior::Fail => self.complete(
                        Value::Failure(TerminalFailure {
                            code: u16::try_from(error.code).unwrap_or(500),
                            message: error.message,
                            metadata: vec![],
                        }),
                        AttemptOutcome::FailedInvocation { code: error.code },
                    ),
                }
                return Ok(());
            }
            MessageType::Shutdown => {
                let shutdown = decode!(ShutdownMessage);
                let attempt = self.attempt.as_ref().unwrap();
                if shutdown.processed_notifications > attempt.sent_notifications {
                    return self.violation(format!(
                        "ShutdownMessage says {} notifications were processed, {} were sent",
                        shutdown.processed_notifications, attempt.sent_notifications
                    ));
                }
                self.restart_or_suspend(Some(shutdown.processed_notifications));
                return Ok(());
            }
            _ => {}
        }

        let mut results = vec![];
        let outcome = match ty {
            MessageType::GetStateAction => {
                let a = decode!(GetStateActionMessage);
                self.check_common(a.parent_span_id)
                    .and_then(|_| self.check_ids(&[(a.result_id, true)]))
                    .and_then(|_| self.check_scope(a.scope, false))
                    .map(|scope| {
                        let key = String::from_utf8_lossy(&a.key).into_owned();
                        let value = match self.scope_state(scope).get(&key) {
                            Some(value) => ResultVariant::Value(value.clone().into()),
                            None => ResultVariant::Void(Default::default()),
                        };
                        results.push((a.result_id, value));
                    })
            }
            MessageType::GetStateKeysAction => {
                let a = decode!(GetStateKeysActionMessage);
                self.check_common(a.parent_span_id)
                    .and_then(|_| self.check_ids(&[(a.result_id, true)]))
                    .and_then(|_| self.check_scope(a.scope, false))
                    .map(|scope| {
                        let keys = self
                            .scope_state(scope)
                            .keys()
                            .map(|key| Bytes::from(key.clone().into_bytes()))
                            .collect();
                        results.push((a.result_id, ResultVariant::StateKeys(StateKeys { keys })));
                    })
            }
            MessageType::FlushAction => {
                let a = decode!(FlushActionMessage);
                self.check_common(a.parent_span_id)
                    .and_then(|_| self.check_ids(&[(a.result_id, true)]))
                    .map(|_| results.push((a.result_id, void())))
            }
            MessageType::AttachInvocationAction => {
                let a = decode!(AttachInvocationActionMessage);
                self.check_common(a.parent_span_id)
                    .and_then(|_| self.check_ids(&[(a.result_id, true)]))
                    .and_then(|_| self.check_attach(&a))
                    .map(|_| self.attach(a, &mut results))
            }
            MessageType::ReadSignalAction => {
                let a = decode!(ReadSignalActionMessage);
                self.check_common(a.parent_span_id)
                    .and_then(|_| self.check_ids(&[(a.result_id, true)]))
                    .map(|_| self.read_signal(a, &mut results))
            }
            MessageType::AtomicGroupAction => {
                let group = decode!(AtomicGroupActionMessage);
                self.check_group(&group).map(|_| {
                    let result_id = group.result_id;
                    for action in group.actions {
                        self.apply_groupable(action.action.unwrap(), &mut results);
                    }
                    results.push((result_id, void()));
                })
            }
            _ => {
                let groupable = match ty {
                    MessageType::SetStateAction => {
                        Grouped::SetState(decode!(SetStateActionMessage))
                    }
                    MessageType::ClearStateAction => {
                        Grouped::ClearState(decode!(ClearStateActionMessage))
                    }
                    MessageType::ClearAllStateAction => {
                        Grouped::ClearAllState(decode!(ClearAllStateActionMessage))
                    }
                    MessageType::CallAction => Grouped::Call(decode!(CallActionMessage)),
                    MessageType::CancelInvocationAction => {
                        Grouped::CancelInvocation(decode!(CancelInvocationActionMessage))
                    }
                    MessageType::SendSignalAction => {
                        Grouped::SendSignal(decode!(SendSignalActionMessage))
                    }
                    MessageType::SpanStartAction => {
                        Grouped::SpanStart(decode!(SpanStartActionMessage))
                    }
                    MessageType::SpanEndAction => Grouped::SpanEnd(decode!(SpanEndActionMessage)),
                    ty => unreachable!("{ty:?} is checked at receive"),
                };
                self.check_groupables(std::slice::from_ref(&groupable), &[])
                    .map(|_| self.apply_groupable(groupable, &mut results))
            }
        };
        if let Err(what) = outcome {
            return self.violation(what);
        }
        self.attempt.as_mut().unwrap().actions_applied += 1;
        for (result_id, result) in results {
            self.result(result_id, result);
        }
        Ok(())
    }

    /// Send a result, unless nobody wants it (id 0). After the runtime closed its side it cannot
    /// be sent, and counts for rule 3.
    fn result(&mut self, result_id: u32, result: ResultVariant) {
        if result_id == 0 {
            return;
        }
        let attempt = self.attempt.as_mut().unwrap();
        assert!(
            attempt.open_result_ids.remove(&result_id),
            "the fake runtime sent a second result for {result_id} (G7)"
        );
        if attempt.closed.is_some() {
            attempt.arrived_after_close += 1;
            return;
        }
        attempt.sent_notifications += 1;
        attempt
            .out
            .push(messages::encode(&ResultNotificationMessage {
                result_id,
                result: Some(result),
            }));
    }

    fn wait(&mut self, result_id: u32, on: WaitOn) {
        let attempt = self.attempt.as_ref().unwrap().number;
        self.waits.push(Wait {
            attempt,
            result_id,
            on,
        });
    }

    fn scope_state(&mut self, scope: messages::StateScope) -> &mut BTreeMap<String, Bytes> {
        match scope {
            messages::StateScope::Invocation => &mut self.invocation_state,
            _ => &mut self.object_state,
        }
    }

    // --- Checks: spec 12, by the SDK. Each returns what is wrong.

    /// The ids of one action or group, in wire order: `(id, required)`. Each must be above every
    /// id used before, and at least `next_result_id`; 0 is absent.
    fn check_ids(&mut self, ids: &[(u32, bool)]) -> Result<(), String> {
        let attempt = self.attempt.as_mut().unwrap();
        let mut highest = attempt.highest_result_id;
        for &(id, required) in ids {
            if id == 0 {
                if required {
                    return Err("a required result id is absent".to_owned());
                }
                continue;
            }
            if id <= highest {
                return Err(format!(
                    "result id {id} is not above every id used before ({highest}), or is below next_result_id"
                ));
            }
            highest = id;
        }
        attempt.highest_result_id = highest;
        for &(id, _) in ids {
            if id != 0 {
                attempt.open_result_ids.insert(id);
            }
        }
        Ok(())
    }

    fn check_common(&self, parent_span_id: u32) -> Result<(), String> {
        self.check_parent(parent_span_id, &HashSet::new())
    }

    fn check_parent(&self, parent_span_id: u32, extra: &HashSet<u32>) -> Result<(), String> {
        let spans = &self.attempt.as_ref().unwrap().spans;
        if parent_span_id != 0
            && !spans.contains(&parent_span_id)
            && !extra.contains(&parent_span_id)
        {
            return Err(format!(
                "parent_span_id {parent_span_id} names no span this attempt started"
            ));
        }
        Ok(())
    }

    fn check_scope(&self, scope: i32, write: bool) -> Result<messages::StateScope, String> {
        match messages::StateScope::try_from(scope) {
            Ok(messages::StateScope::Invocation) => Ok(messages::StateScope::Invocation),
            Ok(messages::StateScope::Object) => {
                if self.spec.key.is_none() {
                    Err("object state, where the target has no key".to_owned())
                } else if write && self.spec.shared {
                    Err("a write of object state from a shared handler".to_owned())
                } else {
                    Ok(messages::StateScope::Object)
                }
            }
            _ => Err(format!("{scope} is not a state scope")),
        }
    }

    fn check_attach(&self, a: &AttachInvocationActionMessage) -> Result<(), String> {
        match &a.target {
            None => Err("AttachInvocation with no target".to_owned()),
            Some(AttachTarget::InvocationId(id)) => check_invocation_id(id),
            Some(_) => Ok(()),
        }
    }

    fn check_group(&mut self, group: &AtomicGroupActionMessage) -> Result<(), String> {
        if group.actions.is_empty() {
            return Err("an atomic group with no member".to_owned());
        }
        let mut members = vec![];
        for action in &group.actions {
            match &action.action {
                Some(action) => members.push(action.clone()),
                None => return Err("a member of an atomic group with no action".to_owned()),
            }
        }
        // The group's own fields: its parent is a span started before it.
        self.check_common(group.parent_span_id)?;
        // The members' ids in their order, then the group's own.
        self.check_groupables(&members, &[(group.result_id, false)])
    }

    /// Check actions a group can hold, before any is applied (G3), and take their ids, then
    /// `then_ids`.
    fn check_groupables(
        &mut self,
        actions: &[Grouped],
        then_ids: &[(u32, bool)],
    ) -> Result<(), String> {
        let mut ids = vec![];
        let mut new_spans = HashSet::new();
        for action in actions {
            let parent = match action {
                Grouped::SetState(a) => {
                    self.check_scope(a.scope, true)?;
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
                Grouped::ClearState(a) => {
                    self.check_scope(a.scope, true)?;
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
                Grouped::ClearAllState(a) => {
                    self.check_scope(a.scope, true)?;
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
                Grouped::Call(a) => {
                    // The two ids of a call, in either order.
                    let (first, second) =
                        if a.invocation_id_result_id < a.result_id || a.result_id == 0 {
                            ((a.invocation_id_result_id, true), (a.result_id, false))
                        } else {
                            ((a.result_id, false), (a.invocation_id_result_id, true))
                        };
                    ids.push(first);
                    ids.push(second);
                    a.parent_span_id
                }
                Grouped::CancelInvocation(a) => {
                    check_invocation_id(&a.invocation_id)?;
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
                Grouped::SendSignal(a) => {
                    check_invocation_id(&a.target_invocation_id)?;
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
                Grouped::SpanStart(a) => {
                    if a.span_id == 0 {
                        return Err("span id 0".to_owned());
                    }
                    let spans = &self.attempt.as_ref().unwrap().spans;
                    if spans.contains(&a.span_id) || new_spans.contains(&a.span_id) {
                        return Err(format!("span {} started twice", a.span_id));
                    }
                    self.check_parent(a.parent_span_id, &new_spans)?;
                    new_spans.insert(a.span_id);
                    ids.push((a.result_id, false));
                    0
                }
                Grouped::SpanEnd(a) => {
                    let spans = &self.attempt.as_ref().unwrap().spans;
                    if a.span_id == 0
                        || !(spans.contains(&a.span_id) || new_spans.contains(&a.span_id))
                    {
                        return Err(format!(
                            "the end of span {}, which this attempt did not start",
                            a.span_id
                        ));
                    }
                    ids.push((a.result_id, false));
                    a.parent_span_id
                }
            };
            self.check_parent(parent, &new_spans)?;
        }
        ids.extend_from_slice(then_ids);
        self.check_ids(&ids)
    }

    // --- Applying, once checked

    fn apply_groupable(&mut self, action: Grouped, results: &mut Vec<(u32, ResultVariant)>) {
        match action {
            Grouped::SetState(a) => {
                let scope = messages::StateScope::try_from(a.scope).unwrap();
                let key = String::from_utf8_lossy(&a.key).into_owned();
                let value = a.value.map(|value| value.content).unwrap_or_default();
                self.scope_state(scope).insert(key, value);
                results.push((a.result_id, void()));
            }
            Grouped::ClearState(a) => {
                let scope = messages::StateScope::try_from(a.scope).unwrap();
                let key = String::from_utf8_lossy(&a.key).into_owned();
                self.scope_state(scope).remove(&key);
                results.push((a.result_id, void()));
            }
            Grouped::ClearAllState(a) => {
                let scope = messages::StateScope::try_from(a.scope).unwrap();
                self.scope_state(scope).clear();
                results.push((a.result_id, void()));
            }
            Grouped::Call(a) => self.call(a, results),
            Grouped::CancelInvocation(a) => {
                self.cancels_sent.push(a.invocation_id.clone());
                results.push((a.result_id, void()));
                // A callee that still runs ends, as cancelled.
                let pending = self
                    .calls
                    .iter()
                    .any(|call| call.invocation_id == a.invocation_id && call.output.is_none());
                if pending {
                    // Its result comes after this action's own.
                    let id = a.invocation_id;
                    let deferred = std::mem::take(results);
                    for (result_id, result) in deferred {
                        self.result(result_id, result);
                    }
                    let _ = self.complete_call(&id, Value::Failure(cancelled_failure()));
                }
            }
            Grouped::SendSignal(a) => {
                let value = match a.result {
                    None | Some(send_signal_action_message::Result::Void(_)) => Value::Void,
                    Some(send_signal_action_message::Result::Value(v)) => Value::Success(v.content),
                    Some(send_signal_action_message::Result::Failure(f)) => {
                        Value::Failure(f.into())
                    }
                };
                results.push((a.result_id, void()));
                if a.target_invocation_id == self.spec.invocation_id {
                    // To itself: it is a signal this invocation receives.
                    let deferred = std::mem::take(results);
                    for (result_id, result) in deferred {
                        self.result(result_id, result);
                    }
                    self.send_signal(&a.signal_name, value);
                } else {
                    self.signals_sent.push(SentSignal {
                        target_invocation_id: a.target_invocation_id,
                        name: a.signal_name,
                        value,
                    });
                }
            }
            Grouped::SpanStart(a) => {
                self.attempt.as_mut().unwrap().spans.insert(a.span_id);
                results.push((a.result_id, void()));
            }
            Grouped::SpanEnd(a) => results.push((a.result_id, void())),
        }
    }

    fn call(&mut self, a: CallActionMessage, results: &mut Vec<(u32, ResultVariant)>) {
        let attempt = self.attempt.as_ref().unwrap().number;
        // With an idempotency key, a new call reaches the invocation the first one started.
        let existing = a.idempotency_key.as_ref().and_then(|idempotency_key| {
            self.calls.iter().position(|call| {
                call.service == a.service_name
                    && call.handler == a.handler_name
                    && call.key == a.key
                    && call.idempotency_key.as_ref() == Some(idempotency_key)
            })
        });
        let index = match existing {
            Some(index) => index,
            None => {
                let invalid = a.idempotency_key.as_deref() == Some("")
                    || a.scope.as_deref() == Some("")
                    || a.limit_key.as_deref() == Some("")
                    || (a.limit_key.is_some() && a.scope.is_none());
                let behavior = self
                    .callee_behaviors
                    .get(&(a.service_name.clone(), a.handler_name.clone()))
                    .cloned()
                    .unwrap_or(CalleeBehavior::Pending);
                let output = if invalid {
                    Some(Value::Failure(TerminalFailure {
                        code: 400,
                        message: "the target cannot be invoked: invalid key, scope or limit key"
                            .to_owned(),
                        metadata: vec![],
                    }))
                } else {
                    match behavior {
                        CalleeBehavior::Pending => None,
                        CalleeBehavior::Returns(value) => Some(value),
                        CalleeBehavior::NotFound => Some(Value::Failure(TerminalFailure {
                            code: 404,
                            message: format!(
                                "the target cannot be invoked: {}/{} does not exist",
                                a.service_name, a.handler_name
                            ),
                            metadata: vec![],
                        })),
                    }
                };
                self.calls.push(CallRecord {
                    invocation_id: format!("inv_1fakecallee{:018}", self.calls.len() + 1),
                    service: a.service_name.clone(),
                    handler: a.handler_name.clone(),
                    key: a.key.clone(),
                    idempotency_key: a.idempotency_key.clone(),
                    parameter: a.parameter.clone().map(|p| p.content).unwrap_or_default(),
                    invoke_time: a.invoke_time,
                    attempt,
                    output,
                });
                self.calls.len() - 1
            }
        };
        let call = self.calls[index].clone();
        // The invocation id always comes, and comes first.
        results.push((
            a.invocation_id_result_id,
            ResultVariant::InvocationId(call.invocation_id.clone()),
        ));
        if a.result_id == 0 {
            // A send.
            return;
        }
        match call.output {
            Some(output) => results.push((a.result_id, result_variant(output))),
            None => self.wait(a.result_id, WaitOn::Invocation(call.invocation_id)),
        }
    }

    fn attach(
        &mut self,
        a: AttachInvocationActionMessage,
        results: &mut Vec<(u32, ResultVariant)>,
    ) {
        let not_found = |what: &str| {
            ResultVariant::Failure(messages::Failure {
                code: 404,
                message: format!("no such invocation: {what}"),
                metadata: vec![],
            })
        };
        let found = match a.target.unwrap() {
            AttachTarget::InvocationId(id) if id == self.spec.invocation_id => {
                results.push((
                    a.result_id,
                    ResultVariant::Failure(messages::Failure {
                        code: 400,
                        message: "an invocation cannot attach to itself".to_owned(),
                        metadata: vec![],
                    }),
                ));
                return;
            }
            AttachTarget::InvocationId(id) => self
                .calls
                .iter()
                .find(|call| call.invocation_id == id)
                .cloned()
                .ok_or(id),
            AttachTarget::IdempotentRequestTarget(t) => self
                .calls
                .iter()
                .find(|call| {
                    call.service == t.service_name
                        && call.handler == t.handler_name
                        && call.key == t.service_key.clone().unwrap_or_default()
                        && call.idempotency_key.as_ref() == Some(&t.idempotency_key)
                })
                .cloned()
                .ok_or(t.idempotency_key),
            AttachTarget::WorkflowTarget(t) => self
                .calls
                .iter()
                .find(|call| call.service == t.workflow_name && call.key == t.workflow_key)
                .cloned()
                .ok_or(t.workflow_key),
        };
        match found {
            Err(what) => results.push((a.result_id, not_found(&what))),
            Ok(CallRecord {
                output: Some(output),
                ..
            }) => results.push((a.result_id, result_variant(output))),
            Ok(call) => self.wait(a.result_id, WaitOn::Invocation(call.invocation_id)),
        }
    }

    fn read_signal(&mut self, a: ReadSignalActionMessage, results: &mut Vec<(u32, ResultVariant)>) {
        let received = self
            .signals
            .get(&a.signal_name)
            .and_then(|signals| signals.get(a.signal_index as usize))
            .cloned();
        match received {
            Some(value) => results.push((a.result_id, result_variant(value))),
            None => self.wait(a.result_id, WaitOn::Signal(a.signal_name, a.signal_index)),
        }
    }
}

// --- Helpers

fn is_last_message(ty: MessageType) -> bool {
    matches!(
        ty,
        MessageType::End | MessageType::Error | MessageType::Shutdown
    )
}

fn check_invocation_id(id: &str) -> Result<(), String> {
    if id.starts_with("inv_") && id.len() > 4 {
        Ok(())
    } else {
        Err(format!("a malformed invocation id: {id:?}"))
    }
}

fn void() -> ResultVariant {
    ResultVariant::Void(Default::default())
}

fn result_variant(value: Value) -> ResultVariant {
    match value {
        Value::Void => void(),
        Value::Success(content) => ResultVariant::Value(content.into()),
        Value::Failure(failure) => ResultVariant::Failure(failure.into()),
        Value::StateKeys(keys) => ResultVariant::StateKeys(StateKeys {
            keys: keys
                .into_iter()
                .map(|k| Bytes::from(k.into_bytes()))
                .collect(),
        }),
        Value::InvocationId(id) => ResultVariant::InvocationId(id),
    }
}

fn cancelled_failure() -> TerminalFailure {
    TerminalFailure {
        code: 409,
        message: "cancelled".to_owned(),
        metadata: vec![],
    }
}

fn preload(
    state_map: &mut Vec<StateEntry>,
    scope: messages::StateScope,
    state: &BTreeMap<String, Bytes>,
    policy: &Preload,
) {
    let entry = |key: &String, value: Option<&Bytes>| StateEntry {
        scope: scope as i32,
        key: Bytes::from(key.clone().into_bytes()),
        value: value.map(|value| value.clone().into()),
    };
    match policy {
        Preload::All => state_map.extend(state.iter().map(|(k, v)| entry(k, Some(v)))),
        Preload::Lazy => {}
        Preload::Eager(keys) => state_map.extend(keys.iter().map(|k| entry(k, state.get(k)))),
    }
}

/// The headers of the request that opens an attempt, for the VM's `new`.
pub fn request_headers() -> Vec<(String, String)> {
    vec![(
        "content-type".to_owned(),
        ProtocolVersion::V1.content_type().to_owned(),
    )]
}
