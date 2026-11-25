//! The VM against the fake runtime ([`crate::explicit_recovery::fake_runtime`]): the worked
//! examples of spec 14 end to end, the restart and suspend rules of spec 10.5, wake-ups, and the
//! fake's own checks of what an SDK sends.

use crate::explicit_recovery::commit::{begin_commit, end_commit};
use crate::explicit_recovery::fake_runtime::{
    invocation_id, request_headers, AttemptOutcome, CalleeBehavior, FakeRuntime, FakeRuntimeError,
    FakeRuntimeOptions, InvocationSpec, InvocationStatus, Preload, ShutdownReason,
};
use crate::explicit_recovery::{
    ActionOptions, CoreExplicitRecoveryVM, ExplicitRecoveryVM, Notification, Options, StateScope,
    CANCEL_NOTIFICATION_HANDLE,
};
use crate::service_protocol::explicit_recovery::messages::{
    self, AtomicGroupActionMessage, EndMessage, FlushActionMessage, RestateMessage,
    SetStateActionMessage, ShutdownMessage, SpanEndActionMessage,
};
use crate::{AttachInvocationTarget, Error, NonEmptyValue, Target, TerminalFailure, Value};
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use test_log::test;

// --- Harness

fn new_vm() -> CoreExplicitRecoveryVM {
    CoreExplicitRecoveryVM::new(request_headers(), Options::default()).unwrap()
}

/// Start an attempt: a new VM, with its `StartMessage`.
fn start(runtime: &mut FakeRuntime) -> CoreExplicitRecoveryVM {
    let mut vm = new_vm();
    vm.notify_input(runtime.start_attempt().unwrap());
    assert!(vm.is_ready_to_execute().unwrap());
    vm
}

/// Carry bytes both ways until neither side has more, without taking notifications from the VM.
/// Once the runtime closed its side, the VM hears the end of its input.
fn exchange(runtime: &mut FakeRuntime, vm: &mut CoreExplicitRecoveryVM) {
    loop {
        let mut progressed = false;
        let written = vm.take_output();
        if !written.is_empty() {
            runtime.receive(written).unwrap();
            progressed = true;
        }
        let notifications = runtime.take_output();
        if !notifications.is_empty() {
            vm.notify_input(notifications);
            progressed = true;
        }
        if runtime.is_closed() && !vm.should_close_output() {
            vm.notify_input_closed();
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
}

fn take_all(vm: &mut CoreExplicitRecoveryVM) -> Vec<Notification> {
    let mut taken = vec![];
    while let Ok(Some(notification)) = vm.take_next_notification() {
        taken.push(notification);
    }
    taken
}

/// Exchange, then take what the VM has.
fn run(runtime: &mut FakeRuntime, vm: &mut CoreExplicitRecoveryVM) -> Vec<Notification> {
    exchange(runtime, vm);
    take_all(vm)
}

/// The attempt ends as the SDK ends it: the last bytes, then the SDK closes its side (D35).
fn close(runtime: &mut FakeRuntime, vm: &mut CoreExplicitRecoveryVM) {
    exchange(runtime, vm);
    assert!(
        vm.should_close_output(),
        "the SDK closes after the runtime, once its last message is written"
    );
    runtime.sdk_closed().unwrap();
}

fn notification(handle: crate::NotificationHandle, value: Value) -> Notification {
    Notification { handle, value }
}

fn success(v: &'static str) -> Value {
    Value::Success(Bytes::from_static(v.as_bytes()))
}

fn ok(v: &'static str) -> NonEmptyValue {
    NonEmptyValue::Success(Bytes::from_static(v.as_bytes()))
}

fn bytes(v: &'static str) -> Bytes {
    Bytes::from_static(v.as_bytes())
}

fn target(service: &str, handler: &str, idempotency_key: Option<&str>) -> Target {
    Target {
        service: service.to_owned(),
        handler: handler.to_owned(),
        key: None,
        idempotency_key: idempotency_key.map(str::to_owned),
        scope: None,
        limit_key: None,
        headers: vec![],
    }
}

fn charge() -> Target {
    target("Payments", "charge", Some("k"))
}

fn set(vm: &mut CoreExplicitRecoveryVM, scope: StateScope, key: &str, value: &'static str) {
    vm.cmd_state_set(
        scope,
        key.to_owned(),
        bytes(value),
        Default::default(),
        false,
    )
    .unwrap();
}

fn get(vm: &mut CoreExplicitRecoveryVM, scope: StateScope, key: &str) -> crate::NotificationHandle {
    vm.cmd_state_get(scope, key.to_owned(), Default::default())
        .unwrap()
}

fn encode<M: RestateMessage>(m: &M) -> Bytes {
    messages::encode(m)
}

fn concat(parts: &[Bytes]) -> Bytes {
    let mut buf = BytesMut::new();
    for part in parts {
        buf.extend_from_slice(part);
    }
    buf.freeze()
}

// --- Spec 14: the worked examples

#[test]
fn spec_14_a_commit_with_a_send() {
    let mut runtime = FakeRuntime::with_options(
        InvocationSpec::default(),
        FakeRuntimeOptions {
            first_next_result_id: 7,
            ..Default::default()
        },
    );
    let mut vm = start(&mut runtime);

    // transaction { UPDATE …, send(expire) }
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "tail", "t1");
    set(&mut vm, StateScope::Object, "head", "h1");
    let expire = vm
        .cmd_send(
            target("Notes", "expire", None),
            Bytes::new(),
            Some(Duration::from_millis(60_000)),
            Default::default(),
        )
        .unwrap();
    let commit = vm
        .atomic_group_end(ActionOptions::named("commit"), true)
        .unwrap()
        .unwrap();

    // One message and one round trip: result 7, the invocation id; result 8, the commit
    let taken = run(&mut runtime, &mut vm);
    let expire_id = runtime.calls()[0].invocation_id.clone();
    assert_eq!(
        taken,
        vec![
            notification(
                expire.invocation_id_notification_handle,
                Value::InvocationId(expire_id)
            ),
            notification(commit, Value::Void),
        ]
    );
    assert_eq!(runtime.object_state()["tail"], bytes("t1"));
    assert_eq!(runtime.object_state()["head"], bytes("h1"));
    assert_eq!(runtime.calls()[0].invoke_time, 60_000);
    assert_eq!(runtime.next_result_id(), 7, "decided when the attempt ends");

    vm.end(ok("done")).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Completed]);
    assert_eq!(
        runtime.status(),
        &InvocationStatus::Completed(success("done"))
    );
    assert_eq!(runtime.next_result_id(), 9);
    assert_eq!(runtime.violations(), &[] as &[String]);
}

#[test]
fn spec_14_progress_in_invocation_state() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    runtime.script_callee("Payments", "charge", CalleeBehavior::Returns(success("ok")));
    runtime
        .object_state_mut()
        .insert("balance".to_owned(), bytes("100"));

    // The handler: charge once, and remember that it did
    let handler = |runtime: &mut FakeRuntime, vm: &mut CoreExplicitRecoveryVM| {
        let charged = get(vm, StateScope::Invocation, "charged");
        let taken = run(runtime, vm);
        assert_eq!(taken[0].handle, charged);
        if taken[0].value != Value::Void {
            return true;
        }
        let call = vm
            .cmd_call(charge(), bytes("10"), None, Default::default())
            .unwrap();
        let taken = run(runtime, vm);
        assert_eq!(
            taken[1],
            notification(call.call_notification_handle, success("ok"))
        );
        vm.atomic_group_start().unwrap();
        set(vm, StateScope::Object, "balance", "90");
        set(vm, StateScope::Invocation, "charged", "1");
        let commit = vm
            .atomic_group_end(Default::default(), true)
            .unwrap()
            .unwrap();
        assert_eq!(run(runtime, vm), vec![notification(commit, Value::Void)]);
        false
    };

    // Attempt 1, next_result_id 1: invocation state is empty, and not partial
    let mut vm = start(&mut runtime);
    assert!(!handler(&mut runtime, &mut vm));
    // The stream breaks before the handler returns: a failure, then a retry
    runtime.break_stream().unwrap();
    assert!(matches!(
        runtime.outcomes(),
        [AttemptOutcome::Failed { .. }]
    ));
    assert_eq!(runtime.status(), &InvocationStatus::Ready);
    assert_eq!(runtime.next_result_id(), 4);

    // Attempt 2, next_result_id 4: state_map holds INVOCATION charged: no second call
    let mut vm = start(&mut runtime);
    assert_eq!(vm.get_input().unwrap().attempt, 2);
    assert!(handler(&mut runtime, &mut vm));
    vm.end(ok("charged once")).unwrap();
    close(&mut runtime, &mut vm);

    assert_eq!(runtime.calls().len(), 1);
    assert_eq!(runtime.object_state()["balance"], bytes("90"));
    assert_eq!(
        runtime.status(),
        &InvocationStatus::Completed(success("charged once"))
    );
    assert_eq!(runtime.violations(), &[] as &[String]);
}

#[test]
fn spec_14_a_call_that_outlives_its_attempt() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());

    // Attempt 1, next_result_id 1: Call charge (result ids 1, 2)
    let mut vm = start(&mut runtime);
    let call = vm
        .cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    let taken = run(&mut runtime, &mut vm);
    let callee = runtime.calls()[0].invocation_id.clone();
    assert_eq!(
        taken,
        vec![notification(
            call.invocation_id_notification_handle,
            Value::InvocationId(callee.clone())
        )]
    );
    // Silent for the inactivity timeout: 1 sent, 1 processed, result 2 outstanding: rule 6
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    exchange(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Suspended]);
    assert_eq!(runtime.status(), &InvocationStatus::Suspended);
    assert!(vm.should_close_output());
    runtime.sdk_closed().unwrap();

    // The callee returns: a result arrives, the invocation wakes up, the result is dropped
    runtime.complete_call(&callee, success("charged")).unwrap();
    assert_eq!(runtime.status(), &InvocationStatus::Ready);

    // Attempt 2, next_result_id 3: Call charge (result ids 3, 4)
    assert_eq!(runtime.next_result_id(), 3);
    let mut vm = start(&mut runtime);
    let call = vm
        .cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    assert_eq!(
        run(&mut runtime, &mut vm),
        vec![
            // Result 3: the same invocation id. Result 4: what charge returned
            notification(
                call.invocation_id_notification_handle,
                Value::InvocationId(callee)
            ),
            notification(call.call_notification_handle, success("charged")),
        ]
    );
    vm.end(ok("done")).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.calls().len(), 1);
    assert_eq!(runtime.violations(), &[] as &[String]);
}

#[test]
fn spec_14_a_result_the_sdk_never_got() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());

    // The handler waits on the first of a call (result id 2) and a signal (result id 3)
    let mut vm = start(&mut runtime);
    vm.cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    vm.cmd_signal_read("approval".to_owned(), 0, Default::default())
        .unwrap();
    assert_eq!(run(&mut runtime, &mut vm).len(), 1);
    // The SDK is gone, but the runtime has not noticed yet: result 2 is never processed
    let callee = runtime.calls()[0].invocation_id.clone();
    runtime.complete_call(&callee, success("charged")).unwrap();
    let _lost = runtime.take_output();
    // Silent for the inactivity timeout: shutdown, and no last message in time: rule 2
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    runtime.shutdown_answer_timed_out().unwrap();
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 2 }]);
    assert_eq!(runtime.status(), &InvocationStatus::Ready);
    drop(vm);

    // Attempt 2: with an idempotency key, the call returns at once
    let mut vm = start(&mut runtime);
    let call = vm
        .cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    let taken = run(&mut runtime, &mut vm);
    assert_eq!(
        taken[1],
        notification(call.call_notification_handle, success("charged"))
    );
}

#[test]
fn spec_14_a_shutdown_that_crosses_an_action() {
    let mut runtime = FakeRuntime::with_options(
        InvocationSpec::default(),
        FakeRuntimeOptions {
            first_next_result_id: 4,
            ..Default::default()
        },
    );
    // A Call is outstanding (result id 5); the handler is silent
    let mut vm = start(&mut runtime);
    let call = vm
        .cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    assert_eq!(run(&mut runtime, &mut vm).len(), 1);
    let _ = call;
    // The handler makes an action, which has not left yet when the runtime shuts it down
    set(&mut vm, StateScope::Invocation, "cursor", "7");
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    vm.notify_input_closed();
    // SetState INVOCATION cursor, then ShutdownMessage: all notifications processed
    runtime.receive(vm.take_output()).unwrap();
    // The SetState is applied; an action came after the close: rule 4
    assert_eq!(runtime.invocation_state()["cursor"], bytes("7"));
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 4 }]);
    runtime.sdk_closed().unwrap();
    assert_eq!(runtime.violations(), &[] as &[String]);
}

// --- Spec 10.5: restart or suspend

/// An attempt that waits on a call, then is shut down for inactivity.
fn waiting_on_a_call(runtime: &mut FakeRuntime) -> CoreExplicitRecoveryVM {
    let mut vm = start(runtime);
    vm.cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    assert_eq!(run(runtime, &mut vm).len(), 1);
    vm
}

#[test]
fn rule_1_a_shutdown_for_the_runtimes_own_reason_or_unasked_restarts() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = waiting_on_a_call(&mut runtime);
    runtime.shut_down(ShutdownReason::Own).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 1 }]);

    // The SDK goes away: an unasked ShutdownMessage, and the runtime closes after it
    let mut vm = waiting_on_a_call(&mut runtime);
    vm.shutdown();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes()[1], AttemptOutcome::Restarted { rule: 1 });
    assert_eq!(runtime.violations(), &[] as &[String]);
}

#[test]
fn rule_2_no_last_message_after_the_shutdown_restarts() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let _vm = waiting_on_a_call(&mut runtime);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    runtime.sdk_closed().unwrap();
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 2 }]);

    let _vm = waiting_on_a_call(&mut runtime);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    runtime.break_stream().unwrap();
    assert_eq!(runtime.outcomes()[1], AttemptOutcome::Restarted { rule: 2 });

    // Before the runtime closed its side, the same end is a failure
    let _vm = waiting_on_a_call(&mut runtime);
    runtime.sdk_closed().unwrap();
    assert!(matches!(
        runtime.outcomes()[2],
        AttemptOutcome::Failed { .. }
    ));
}

#[test]
fn rule_3_a_notification_not_taken_restarts() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = start(&mut runtime);
    vm.cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    // The invocation id reaches the VM, but the handler never takes it
    exchange(&mut runtime, &mut vm);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 3 }]);

    // The cancel signal arrives after the runtime closed its side: it could not be sent, and
    // counts as not processed
    let mut vm = waiting_on_a_call(&mut runtime);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    runtime.cancel();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes()[1], AttemptOutcome::Restarted { rule: 3 });
    // The next attempt learns of it from its start
    let vm = start(&mut runtime);
    assert!(vm.is_cancelled());
}

#[test]
fn rule_4_a_heartbeat_that_crosses_the_shutdown_restarts() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = waiting_on_a_call(&mut runtime);
    vm.heartbeat().unwrap();
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 4 }]);
}

#[test]
fn rule_5_nothing_outstanding_restarts_and_rule_6_suspends() {
    let mut runtime = FakeRuntime::new(InvocationSpec {
        object_preload: Preload::Lazy,
        ..Default::default()
    });
    // A read and a flush are internal: answered, taken, nothing outstanding
    let mut vm = start(&mut runtime);
    get(&mut vm, StateScope::Object, "head");
    vm.cmd_flush(Default::default()).unwrap();
    assert_eq!(run(&mut runtime, &mut vm).len(), 2);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Restarted { rule: 5 }]);

    // A signal read nothing has sent yet is outstanding: suspend
    let mut vm = start(&mut runtime);
    vm.cmd_signal_read("approval".to_owned(), 0, Default::default())
        .unwrap();
    exchange(&mut runtime, &mut vm);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes()[1], AttemptOutcome::Suspended);

    // Anything wakes it: here a signal of another name. A new attempt reads again
    runtime.send_signal("other", Value::Void);
    assert_eq!(runtime.status(), &InvocationStatus::Ready);
    runtime.send_signal("approval", success("yes"));
    let mut vm = start(&mut runtime);
    let approval = vm
        .cmd_signal_read("approval".to_owned(), 0, Default::default())
        .unwrap();
    assert_eq!(
        run(&mut runtime, &mut vm),
        vec![notification(approval, success("yes"))]
    );
}

#[test]
fn idle_restarts_are_bounded() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let idle_attempt = |runtime: &mut FakeRuntime| {
        let mut vm = start(runtime);
        // Answered locally: nothing goes on the wire, no action is applied
        get(&mut vm, StateScope::Object, "head");
        take_all(&mut vm);
        runtime.shut_down(ShutdownReason::Inactivity).unwrap();
        close(runtime, &mut vm);
        runtime.outcomes().last().unwrap().clone()
    };
    for _ in 0..3 {
        assert_eq!(
            idle_attempt(&mut runtime),
            AttemptOutcome::Restarted { rule: 5 }
        );
    }
    // The fourth in a row is a failure, and so is the next
    assert!(matches!(
        idle_attempt(&mut runtime),
        AttemptOutcome::Failed { .. }
    ));
    assert!(matches!(
        idle_attempt(&mut runtime),
        AttemptOutcome::Failed { .. }
    ));

    // An attempt with an action applied ends the streak
    let mut vm = start(&mut runtime);
    vm.cmd_flush(Default::default()).unwrap();
    run(&mut runtime, &mut vm);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(
        runtime.outcomes().last().unwrap(),
        &AttemptOutcome::Restarted { rule: 5 }
    );
    assert_eq!(
        idle_attempt(&mut runtime),
        AttemptOutcome::Restarted { rule: 5 }
    );
}

// --- Spec 11: cancellation

#[test]
fn the_cancel_signal_is_a_counted_notification_and_wakes_once() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = waiting_on_a_call(&mut runtime);
    runtime.cancel();
    assert_eq!(
        run(&mut runtime, &mut vm),
        vec![notification(CANCEL_NOTIFICATION_HANDLE, Value::Void)]
    );
    assert!(vm.is_cancelled());
    // The handler cancels its call, and waits for it to end
    let callee = runtime.calls()[0].invocation_id.clone();
    vm.cmd_cancel_invocation(callee.clone(), Default::default(), false)
        .unwrap();
    let taken = run(&mut runtime, &mut vm);
    assert_eq!(taken.len(), 1);
    assert!(matches!(&taken[0].value, Value::Failure(f) if f.code == 409));
    assert_eq!(runtime.cancels_sent(), &[callee]);
    // It now waits for a signal; shut down for inactivity: processed what was sent, suspend
    vm.cmd_signal_read("never".to_owned(), 0, Default::default())
        .unwrap();
    exchange(&mut runtime, &mut vm);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Suspended]);

    // A second cancel signal changes nothing, and wakes nothing
    runtime.cancel();
    assert_eq!(runtime.status(), &InvocationStatus::Suspended);
    runtime.send_signal("other", Value::Void);
    assert_eq!(runtime.status(), &InvocationStatus::Ready);
    let vm = start(&mut runtime);
    assert!(vm.is_cancelled());
}

#[test]
fn a_suspended_invocation_wakes_on_the_cancel_signal_once() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = waiting_on_a_call(&mut runtime);
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.status(), &InvocationStatus::Suspended);
    runtime.cancel();
    assert_eq!(runtime.status(), &InvocationStatus::Ready);

    // The new attempt knows from its start, gets no notification, keeps waiting: suspended again
    let mut vm = waiting_on_a_call(&mut runtime);
    assert!(vm.is_cancelled());
    runtime.shut_down(ShutdownReason::Inactivity).unwrap();
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.status(), &InvocationStatus::Suspended);
    // The result of the first attempt's call wakes it
    let callee = runtime.calls()[0].invocation_id.clone();
    runtime.complete_call(&callee, success("x")).unwrap();
    assert_eq!(runtime.status(), &InvocationStatus::Ready);
}

#[test]
fn an_invocation_cancelled_before_its_first_attempt_never_starts() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    runtime.cancel();
    assert!(matches!(
        runtime.status(),
        InvocationStatus::Completed(Value::Failure(TerminalFailure { code: 409, .. }))
    ));
    assert!(runtime.start_attempt().is_err());
}

// --- Errors, pauses, lost attempts, G2 and G8

#[test]
fn error_behaviors_retry_pause_and_fail() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = start(&mut runtime);
    vm.notify_error(
        Error::internal("retry me").with_next_retry_delay_override(Duration::from_secs(2)),
    );
    close(&mut runtime, &mut vm);
    assert!(matches!(
        runtime.outcomes()[0],
        AttemptOutcome::Failed { .. }
    ));
    assert_eq!(runtime.last_retry_delay(), Some(2000));

    let mut vm = start(&mut runtime);
    vm.notify_error(Error::internal("pause me").with_should_pause(true));
    close(&mut runtime, &mut vm);
    assert_eq!(runtime.outcomes()[1], AttemptOutcome::Paused { code: 500 });
    assert_eq!(runtime.status(), &InvocationStatus::Paused);
    runtime.resume().unwrap();

    // A violation of the protocol by the runtime: the VM answers 571, a failed attempt
    let mut vm = start(&mut runtime);
    vm.notify_input(encode(&messages::ResultNotificationMessage {
        result_id: 99,
        result: None,
    }));
    close(&mut runtime, &mut vm);
    assert!(
        matches!(&runtime.outcomes()[2], AttemptOutcome::Failed { reason } if reason.contains("571"))
    );
}

#[test]
fn a_lost_attempt_keeps_a_prefix_of_its_actions() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    runtime.set_auto_apply(false);
    let mut vm = start(&mut runtime);
    set(&mut vm, StateScope::Object, "a", "1");
    set(&mut vm, StateScope::Object, "b", "2");
    let flushed = vm.cmd_flush(Default::default()).unwrap();
    runtime.receive(vm.take_output()).unwrap();
    // Received, not applied: no result yet (G8), and the attempt is not silent
    assert!(runtime.take_output().is_empty());
    assert!(runtime.shut_down(ShutdownReason::Inactivity).is_err());
    assert_eq!(runtime.apply_pending(1).unwrap(), 1);
    runtime.lose_attempt().unwrap();
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Lost]);
    assert_eq!(runtime.object_state().len(), 1);
    assert_eq!(runtime.object_state()["a"], bytes("1"));
    let _ = flushed;
    // The ids of what was lost may be used again: none of its results can still arrive
    assert_eq!(runtime.next_result_id(), 1);
}

// --- The commit path

#[test]
fn a_commit_is_one_group_and_what_exceeds_the_budget_leaves_first() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = start(&mut runtime);
    let puts = vec![
        ("tail/1".to_owned(), Bytes::from(vec![1u8; 100])),
        ("tail/2".to_owned(), Bytes::from(vec![2u8; 100])),
        ("tail/3".to_owned(), Bytes::from(vec![3u8; 10])),
    ];
    begin_commit(
        &mut vm,
        StateScope::Object,
        puts,
        &["tail/0".to_owned()],
        "head",
        bytes("h2"),
        150,
    )
    .unwrap();
    // A send made by the transaction's callbacks lands in the group
    vm.cmd_send(
        target("Notes", "expire", None),
        Bytes::new(),
        None,
        Default::default(),
    )
    .unwrap();
    let commit = end_commit(&mut vm).unwrap();
    let written = vm.take_output();

    let mut decoder = crate::service_protocol::explicit_recovery::Decoder::for_registry();
    decoder.push(written.clone());
    let mut types = vec![];
    while let Some(raw) = decoder.consume_next().unwrap() {
        types.push(raw.raw_header().ty());
        if raw.raw_header().ty() == SetStateActionMessage::ty() {
            let set: SetStateActionMessage = prost::Message::decode(raw.into_payload()).unwrap();
            assert_eq!(set.key, Bytes::from_static(b"tail/1"));
            assert_eq!(set.record, messages::RecordLevel::RecordNone as i32);
        } else {
            let group: AtomicGroupActionMessage =
                prost::Message::decode(raw.into_payload()).unwrap();
            assert_eq!(group.actions.len(), 5);
            assert_eq!(group.name, "commit");
        }
    }
    assert_eq!(
        types,
        vec![SetStateActionMessage::ty(), AtomicGroupActionMessage::ty()]
    );

    runtime
        .object_state_mut()
        .insert("tail/0".to_owned(), bytes("old"));
    runtime.receive(written).unwrap();
    vm.notify_input(runtime.take_output());
    let taken = take_all(&mut vm);
    assert_eq!(taken.last().unwrap(), &notification(commit, Value::Void));
    assert_eq!(
        runtime.object_state().keys().collect::<Vec<_>>(),
        vec!["head", "tail/1", "tail/2", "tail/3"]
    );
}

// --- The fake's checks of what the SDK sends (spec 12)

fn assert_violation(bytes: Bytes, spec: InvocationSpec) {
    let mut runtime = FakeRuntime::new(spec);
    let start = runtime.start_attempt().unwrap();
    let _ = start;
    let result = runtime.receive(bytes);
    assert!(
        matches!(result, Err(FakeRuntimeError::Violation(_))),
        "expected a violation, got {result:?}"
    );
    assert!(matches!(
        runtime.outcomes(),
        [AttemptOutcome::Failed { .. }]
    ));
    assert_eq!(runtime.violations().len(), 1);
}

#[test]
fn the_fake_finds_what_an_sdk_must_not_send() {
    let flush = |result_id| {
        encode(&FlushActionMessage {
            result_id,
            ..Default::default()
        })
    };
    let set_state = |scope: i32, result_id| {
        encode(&SetStateActionMessage {
            scope,
            key: bytes("k"),
            result_id,
            ..Default::default()
        })
    };
    let object = messages::StateScope::Object as i32;
    let spec = InvocationSpec::default;

    // A result id used twice, and one not above the last
    assert_violation(concat(&[flush(1), flush(1)]), spec());
    assert_violation(concat(&[flush(2), flush(1)]), spec());
    // A required result id left out
    assert_violation(flush(0), spec());
    // No scope, an unknown scope
    assert_violation(set_state(0, 0), spec());
    assert_violation(set_state(9, 0), spec());
    // Object state where the target has no key; an object write from a shared handler
    assert_violation(
        set_state(object, 0),
        InvocationSpec {
            key: None,
            ..spec()
        },
    );
    assert_violation(
        set_state(object, 0),
        InvocationSpec {
            shared: true,
            ..spec()
        },
    );
    // An empty group, a member with no action
    assert_violation(encode(&AtomicGroupActionMessage::default()), spec());
    assert_violation(
        encode(&AtomicGroupActionMessage {
            actions: vec![Default::default()],
            ..Default::default()
        }),
        spec(),
    );
    // A group's ids out of wire order: the group's own before its members'
    assert_violation(
        encode(&AtomicGroupActionMessage {
            actions: vec![messages::atomic_group_action_message::Action {
                action: Some(
                    messages::atomic_group_action_message::action::Action::SetState(
                        SetStateActionMessage {
                            scope: object,
                            result_id: 5,
                            ..Default::default()
                        },
                    ),
                ),
            }],
            result_id: 4,
            ..Default::default()
        }),
        spec(),
    );
    // The end of a span it did not start; a parent it did not start
    assert_violation(
        encode(&SpanEndActionMessage {
            span_id: 3,
            ..Default::default()
        }),
        spec(),
    );
    assert_violation(
        encode(&FlushActionMessage {
            result_id: 1,
            parent_span_id: 2,
            ..Default::default()
        }),
        spec(),
    );
    // A malformed invocation id, or scope: the runtime parses them out of the action. The VM
    // refuses what user code passes before it sends it (D65), so this is an SDK bug.
    for malformed in ["inv_garbage", "inv_1fakecallee000000000000000001", ""] {
        assert_violation(
            encode(&messages::CancelInvocationActionMessage {
                invocation_id: malformed.to_owned(),
                ..Default::default()
            }),
            spec(),
        );
        assert_violation(
            encode(&messages::SendSignalActionMessage {
                target_invocation_id: malformed.to_owned(),
                signal_name: "go".to_owned(),
                ..Default::default()
            }),
            spec(),
        );
        assert_violation(
            encode(&messages::AttachInvocationActionMessage {
                target: Some(
                    messages::attach_invocation_action_message::Target::InvocationId(
                        malformed.to_owned(),
                    ),
                ),
                result_id: 1,
                ..Default::default()
            }),
            spec(),
        );
    }
    assert_violation(
        encode(&messages::AttachInvocationActionMessage {
            target: Some(
                messages::attach_invocation_action_message::Target::WorkflowTarget(
                    messages::WorkflowTarget {
                        workflow_name: "Wf".to_owned(),
                        workflow_key: "k".to_owned(),
                        scope: Some("a/b".to_owned()),
                    },
                ),
            ),
            result_id: 1,
            ..Default::default()
        }),
        spec(),
    );
    // EndMessage without an output
    assert_violation(encode(&EndMessage::default()), spec());
    // More notifications processed than sent
    assert_violation(
        encode(&ShutdownMessage {
            processed_notifications: 1,
        }),
        spec(),
    );
    // A reserved bit, an unknown type, a message of the runtime's
    assert_violation(Bytes::from_static(&[0x04, 0x40, 0, 1, 0, 0, 0, 0]), spec());
    assert_violation(Bytes::from_static(&[0x04, 0x00, 0, 0, 0, 0, 0, 0]), spec());
    assert_violation(
        encode(&messages::ResultNotificationMessage::default()),
        spec(),
    );
}

#[test]
fn the_fake_mints_invocation_ids_in_restates_format_and_the_vm_checks_user_input() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = start(&mut runtime);
    assert_eq!(vm.get_input().unwrap().invocation_id, invocation_id(0));
    let first = vm
        .cmd_call(charge(), Bytes::new(), None, Default::default())
        .unwrap();
    let second = vm
        .cmd_call(
            target("Other", "run", None),
            Bytes::new(),
            None,
            Default::default(),
        )
        .unwrap();
    run(&mut runtime, &mut vm);
    let ids: Vec<_> = runtime
        .calls()
        .iter()
        .map(|c| c.invocation_id.clone())
        .collect();
    assert_eq!(ids, vec![invocation_id(1), invocation_id(2)]);
    assert_eq!(ids[0].len(), 38);
    assert_ne!(ids[0], ids[1]);

    // User code passes a malformed id: that action fails, nothing reaches the runtime
    for malformed in ["inv_garbage", "inv_1fakecallee000000000000000001"] {
        let refused = vm
            .cmd_cancel_invocation(malformed.to_owned(), Default::default(), false)
            .unwrap_err();
        assert!(refused.is_invalid_input(), "{refused}");
        assert!(vm
            .cmd_signal_send(
                malformed.to_owned(),
                "go".to_owned(),
                None,
                Default::default(),
                false
            )
            .unwrap_err()
            .is_invalid_input());
        assert!(vm
            .cmd_attach_invocation(
                AttachInvocationTarget::InvocationId(malformed.to_owned()),
                Default::default()
            )
            .unwrap_err()
            .is_invalid_input());
    }
    assert!(vm.take_output().is_empty());

    // The ids the fake minted are good: the attempt cancels one and signals the other
    vm.cmd_cancel_invocation(ids[0].clone(), Default::default(), false)
        .unwrap();
    vm.cmd_signal_send(
        invocation_id(9),
        "go".to_owned(),
        None,
        Default::default(),
        false,
    )
    .unwrap();
    let flushed = vm.cmd_flush(Default::default()).unwrap();
    let taken = run(&mut runtime, &mut vm);
    assert!(taken.iter().any(|n| n.handle == flushed));
    assert_eq!(runtime.cancels_sent(), &[ids[0].clone()]);
    assert_eq!(
        runtime.signals_sent()[0].target_invocation_id,
        invocation_id(9)
    );
    assert_eq!(runtime.violations(), &[] as &[String]);
    let _ = (first, second);
}

#[test]
fn what_follows_the_last_message_is_dropped_and_the_last_message_stands() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    let mut vm = start(&mut runtime);
    vm.end(ok("done")).unwrap();
    runtime.receive(vm.take_output()).unwrap();
    assert!(runtime.is_closed());
    assert!(matches!(
        runtime.receive(encode(&messages::HeartbeatMessage {})),
        Err(FakeRuntimeError::Violation(_))
    ));
    assert_eq!(runtime.outcomes(), &[AttemptOutcome::Completed]);
    assert_eq!(
        runtime.status(),
        &InvocationStatus::Completed(success("done"))
    );
}

#[test]
fn preload_policies_reach_the_vm() {
    let mut runtime = FakeRuntime::new(InvocationSpec {
        object_preload: Preload::Eager(vec!["head".to_owned(), "absent".to_owned()]),
        invocation_preload: Preload::All,
        ..Default::default()
    });
    runtime
        .object_state_mut()
        .insert("head".to_owned(), bytes("h"));
    runtime
        .object_state_mut()
        .insert("page".to_owned(), bytes("p"));
    runtime
        .invocation_state_mut()
        .insert("cursor".to_owned(), bytes("3"));
    let mut vm = start(&mut runtime);
    let head = get(&mut vm, StateScope::Object, "head");
    let absent = get(&mut vm, StateScope::Object, "absent");
    let cursor = get(&mut vm, StateScope::Invocation, "cursor");
    let none = get(&mut vm, StateScope::Invocation, "none");
    // Answered from the preload: nothing on the wire
    assert!(vm.take_output().is_empty());
    // A key of a partial scope that is not preloaded: asked
    let page = get(&mut vm, StateScope::Object, "page");
    assert_eq!(
        run(&mut runtime, &mut vm),
        vec![
            notification(head, success("h")),
            notification(absent, Value::Void),
            notification(cursor, success("3")),
            notification(none, Value::Void),
            notification(page, success("p")),
        ]
    );
}

#[test]
fn what_came_before_a_violation_is_applied() {
    let mut runtime = FakeRuntime::new(InvocationSpec::default());
    runtime.start_attempt().unwrap();
    let set = encode(&SetStateActionMessage {
        scope: messages::StateScope::Object as i32,
        key: bytes("k"),
        value: Some(bytes("v").into()),
        ..Default::default()
    });
    let bad = encode(&FlushActionMessage::default());
    assert!(runtime.receive(concat(&[set, bad])).is_err());
    assert_eq!(runtime.object_state()["k"], bytes("v"));
    assert!(matches!(
        runtime.outcomes(),
        [AttemptOutcome::Failed { .. }]
    ));
}
