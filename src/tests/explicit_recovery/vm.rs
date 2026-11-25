//! Tests of [`crate::explicit_recovery`], in the shape of `tests/explicit_recovery`: the test plays the
//! runtime by hand, message by message.

use crate::explicit_recovery::{
    missing_features, ActionOptions, AttemptState, CoreExplicitRecoveryVM, ExplicitRecoveryVM,
    Input, Notification, Options, ProtocolVersion, RecordLevel, StateScope,
};
use crate::service_protocol::explicit_recovery::messages::{
    self, atomic_group_action_message::action::Action as Grouped, end_message,
    result_notification_message, start_message::StateEntry, AtomicGroupActionMessage,
    BuiltInSignal, CallActionMessage, ClearAllStateActionMessage, EndMessage, ErrorMessage,
    FailureMetadata, FlushActionMessage, GetStateActionMessage, HeartbeatMessage,
    ReadSignalActionMessage, RestateMessage, ResultNotificationMessage, SetStateActionMessage,
    ShutdownMessage, SignalNotificationMessage, SpanEndActionMessage, SpanStartActionMessage,
    StartMessage,
};
use crate::service_protocol::explicit_recovery::{Decoder, MessageType};
use crate::vm::errors::codes;
use crate::{
    AttachInvocationTarget, Error, NonEmptyValue, NotificationHandle, Target, Value,
    CANCEL_NOTIFICATION_HANDLE,
};
use bytes::Bytes;
use googletest::prelude::*;
use prost::Message;
use test_log::test;

// --- Test infra

const CONTENT_TYPE_V1: &str = "application/vnd.restate.invocation.explicitrecovery.v1";

fn new_vm() -> CoreExplicitRecoveryVM {
    CoreExplicitRecoveryVM::new(
        vec![("content-type".to_owned(), CONTENT_TYPE_V1.to_owned())],
        Options::default(),
    )
    .unwrap()
}

fn notify<M: RestateMessage>(vm: &mut CoreExplicitRecoveryVM, m: M) {
    vm.notify_input(messages::encode(&m));
}

/// What the VM wrote, message by message.
struct Output(Decoder);

impl Output {
    fn of(vm: &mut CoreExplicitRecoveryVM) -> Self {
        let mut decoder = Decoder::for_registry();
        loop {
            let b = vm.take_output();
            if b.is_empty() {
                break;
            }
            decoder.push(b);
        }
        Self(decoder)
    }

    fn next<M: RestateMessage>(&mut self) -> M {
        let raw = self.0.consume_next().unwrap().expect("a message");
        assert_eq!(raw.raw_header().ty(), M::ty());
        assert_eq!(raw.raw_header().flags(), 0, "the reserved bits are zero");
        M::decode(raw.into_payload()).unwrap()
    }

    fn is_over(&mut self) -> bool {
        self.0.consume_next().unwrap().is_none()
    }
}

fn start_message(next_result_id: u32) -> StartMessage {
    StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "inv_123".to_owned(),
        key: "my-key".to_owned(),
        input: Some(Bytes::from_static(b"my-input").into()),
        attempt: 1,
        next_result_id,
        // Lazy state: what is not in state_map may have a value.
        is_object_scope_partial_state: true,
        is_invocation_scope_partial_state: true,
        ..Default::default()
    }
}

fn running(start: StartMessage) -> CoreExplicitRecoveryVM {
    let mut vm = new_vm();
    assert_eq!(vm.state(), AttemptState::Starting);
    notify(&mut vm, start);
    assert!(vm.is_ready_to_execute().unwrap());
    assert_eq!(vm.state(), AttemptState::Running);
    vm
}

fn result(
    result_id: u32,
    result: result_notification_message::Result,
) -> ResultNotificationMessage {
    ResultNotificationMessage {
        result_id,
        result: Some(result),
    }
}

fn void() -> result_notification_message::Result {
    result_notification_message::Result::Void(Default::default())
}

fn value(v: &'static str) -> result_notification_message::Result {
    result_notification_message::Result::Value(Bytes::from_static(v.as_bytes()).into())
}

fn invocation_id(id: &str) -> result_notification_message::Result {
    result_notification_message::Result::InvocationId(id.to_owned())
}

fn take_all(vm: &mut CoreExplicitRecoveryVM) -> Vec<Notification> {
    let mut notifications = vec![];
    while let Some(notification) = vm.take_next_notification().unwrap() {
        notifications.push(notification);
    }
    notifications
}

fn notification(handle: NotificationHandle, value: Value) -> Notification {
    Notification { handle, value }
}

fn success(v: &'static str) -> Value {
    Value::Success(Bytes::from_static(v.as_bytes()))
}

fn target() -> Target {
    Target {
        service: "MySvc".to_owned(),
        handler: "MyHandler".to_owned(),
        key: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        headers: vec![],
    }
}

fn set(vm: &mut CoreExplicitRecoveryVM, scope: StateScope, key: &str, value: &'static str) {
    assert_eq!(
        vm.cmd_state_set(
            scope,
            key.to_owned(),
            Bytes::from_static(value.as_bytes()),
            Default::default(),
            false
        )
        .unwrap(),
        None
    );
}

/// An invocation id in Restate's format, of another invocation.
const OTHER: &str = "inv_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjSl";

fn is_invalid_input() -> impl Matcher<ActualT = Error> {
    predicate(|e: &Error| e.is_invalid_input() && e.code() == 400)
}

fn is_shut_down() -> impl Matcher<ActualT = Error> {
    predicate(|e: &Error| e.is_suspended_error())
}

// --- Version negotiation

#[test]
fn the_other_family_and_unknown_versions_are_refused_with_415() {
    for content_type in [
        // The durable execution protocol
        "application/vnd.restate.invocation.v8",
        // A version of this family this build does not speak
        "application/vnd.restate.invocation.explicitrecovery.v2",
    ] {
        let error = CoreExplicitRecoveryVM::new(
            vec![("content-type".to_owned(), content_type.to_owned())],
            Options::default(),
        )
        .unwrap_err();
        assert_eq!(error.code(), codes::UNSUPPORTED_MEDIA_TYPE.code());
    }
    // And the durable execution VM refuses this family
    let error = <crate::CoreVM as crate::VM>::new(
        vec![("content-type".to_owned(), CONTENT_TYPE_V1.to_owned())],
        Default::default(),
    )
    .unwrap_err();
    assert_eq!(error.code(), codes::UNSUPPORTED_MEDIA_TYPE.code());

    let head = new_vm().get_response_head();
    assert_eq!(head.version, ProtocolVersion::V1);
    assert_eq!(head.headers[0].value, CONTENT_TYPE_V1);
}

// --- Start

#[test]
fn the_handler_starts_with_the_start_message_alone() {
    let mut vm = running(StartMessage {
        attempt: 3,
        scope: Some("my-scope".to_owned()),
        ..start_message(1)
    });
    assert_eq!(
        vm.get_input().unwrap(),
        Input {
            invocation_id: "inv_123".to_owned(),
            key: "my-key".to_owned(),
            headers: vec![],
            input: Bytes::from_static(b"my-input"),
            scope: Some("my-scope".to_owned()),
            limit_key: None,
            idempotency_key: None,
            attempt: 3,
        }
    );
    vm.end(NonEmptyValue::Success(Bytes::from_static(b"my-output")))
        .unwrap();
    assert_eq!(vm.state(), AttemptState::Closed);

    let mut output = Output::of(&mut vm);
    assert_eq!(
        output.next::<EndMessage>(),
        EndMessage {
            output: Some(end_message::Output::Value(
                Bytes::from_static(b"my-output").into()
            ))
        }
    );
    assert!(output.is_over());
}

// --- Result ids

#[test]
fn result_ids_start_at_next_result_id_and_only_wanted_results_take_one() {
    let mut vm = running(start_message(7));
    // No id: the result is not wanted
    set(&mut vm, StateScope::Object, "a", "1");
    // 7
    let read = vm
        .cmd_state_get(StateScope::Object, "other".to_owned(), Default::default())
        .unwrap();
    // 8 and 9: the invocation id first
    let call = vm
        .cmd_call(target(), Bytes::new(), None, Default::default())
        .unwrap();
    // 10 only: a send is a call without a result id
    let send = vm
        .cmd_send(target(), Bytes::new(), None, Default::default())
        .unwrap();
    // 11
    let written = vm
        .cmd_state_set(
            StateScope::Invocation,
            "cursor".to_owned(),
            Bytes::from_static(b"3"),
            Default::default(),
            true,
        )
        .unwrap()
        .unwrap();

    // Results come in any order; they are handed out in the order they came
    notify(&mut vm, result(11, void()));
    notify(&mut vm, result(9, value("returned")));
    notify(&mut vm, result(8, invocation_id("inv_callee")));
    notify(&mut vm, result(10, invocation_id("inv_sent")));
    notify(&mut vm, result(7, void()));
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(written, Value::Void),
            notification(call.call_notification_handle, success("returned")),
            notification(
                call.invocation_id_notification_handle,
                Value::InvocationId("inv_callee".to_owned())
            ),
            notification(
                send.invocation_id_notification_handle,
                Value::InvocationId("inv_sent".to_owned())
            ),
            notification(read, Value::Void),
        ]
    );

    let mut output = Output::of(&mut vm);
    assert_eq!(output.next::<SetStateActionMessage>().result_id, 0);
    assert_eq!(output.next::<GetStateActionMessage>().result_id, 7);
    let call = output.next::<CallActionMessage>();
    assert_eq!((call.invocation_id_result_id, call.result_id), (8, 9));
    let send = output.next::<CallActionMessage>();
    assert_eq!((send.invocation_id_result_id, send.result_id), (10, 0));
    let set = output.next::<SetStateActionMessage>();
    assert_eq!(set.result_id, 11);
    assert_eq!(set.scope, messages::StateScope::Invocation as i32);
    assert!(output.is_over());
}

// --- State

#[test]
fn state_is_answered_locally_per_scope() {
    let mut vm = running(StartMessage {
        state_map: vec![
            StateEntry {
                scope: messages::StateScope::Object as i32,
                key: Bytes::from_static(b"head"),
                value: Some(Bytes::from_static(b"h1").into()),
            },
            // Known to have no value
            StateEntry {
                scope: messages::StateScope::Object as i32,
                key: Bytes::from_static(b"gone"),
                value: None,
            },
            StateEntry {
                scope: messages::StateScope::Invocation as i32,
                key: Bytes::from_static(b"head"),
                value: Some(Bytes::from_static(b"other").into()),
            },
        ],
        // The object's state is partly there; the invocation's is all there
        is_object_scope_partial_state: true,
        is_invocation_scope_partial_state: false,
        ..start_message(1)
    });
    let get = |vm: &mut CoreExplicitRecoveryVM, scope, key: &str| {
        vm.cmd_state_get(scope, key.to_owned(), Default::default())
            .unwrap()
    };

    let object_head = get(&mut vm, StateScope::Object, "head");
    let object_gone = get(&mut vm, StateScope::Object, "gone");
    let invocation_head = get(&mut vm, StateScope::Invocation, "head");
    let invocation_missing = get(&mut vm, StateScope::Invocation, "missing");
    let invocation_keys = vm
        .cmd_state_get_keys(StateScope::Invocation, Default::default())
        .unwrap();
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(object_head, success("h1")),
            notification(object_gone, Value::Void),
            notification(invocation_head, success("other")),
            notification(invocation_missing, Value::Void),
            notification(invocation_keys, Value::StateKeys(vec!["head".to_owned()])),
        ]
    );
    // Not there, in a scope partly there: asked
    get(&mut vm, StateScope::Object, "missing");
    // None of the local answers counts as a notification of the runtime
    vm.notify_input_closed();

    let mut output = Output::of(&mut vm);
    let asked = output.next::<GetStateActionMessage>();
    assert_eq!(asked.key, Bytes::from_static(b"missing"));
    assert_eq!(asked.scope, messages::StateScope::Object as i32);
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 0
        }
    );
    assert!(output.is_over());
}

#[test]
fn a_large_written_value_is_not_kept_and_is_asked_for() {
    let mut vm = CoreExplicitRecoveryVM::new(
        vec![("content-type".to_owned(), CONTENT_TYPE_V1.to_owned())],
        Options {
            keep_written_values_up_to: 4,
            ..Default::default()
        },
    )
    .unwrap();
    notify(
        &mut vm,
        StartMessage {
            // All of the object's state is there: it is empty
            is_object_scope_partial_state: false,
            ..start_message(1)
        },
    );
    set(&mut vm, StateScope::Object, "small", "tiny");
    set(&mut vm, StateScope::Object, "page", "a whole page");
    let small = vm
        .cmd_state_get(StateScope::Object, "small".to_owned(), Default::default())
        .unwrap();
    let page = vm
        .cmd_state_get(StateScope::Object, "page".to_owned(), Default::default())
        .unwrap();
    // The keys are known all the same
    let keys = vm
        .cmd_state_get_keys(StateScope::Object, Default::default())
        .unwrap();
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(small, success("tiny")),
            notification(
                keys,
                Value::StateKeys(vec!["page".to_owned(), "small".to_owned()])
            ),
        ]
    );
    notify(&mut vm, result(1, value("a whole page")));
    assert_eq!(
        take_all(&mut vm),
        vec![notification(page, success("a whole page"))]
    );

    let mut output = Output::of(&mut vm);
    output.next::<SetStateActionMessage>();
    output.next::<SetStateActionMessage>();
    assert_eq!(
        output.next::<GetStateActionMessage>().key,
        Bytes::from_static(b"page")
    );
    assert!(output.is_over());
}

// --- Atomic groups

#[test]
fn a_group_leaves_as_one_message_with_its_own_result() {
    let mut vm = running(start_message(7));
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "tail", "t");
    set(&mut vm, StateScope::Object, "head", "h");
    let send = vm
        .cmd_send(target(), Bytes::new(), None, Default::default())
        .unwrap();
    // Nothing left yet, and a heartbeat goes around the group
    vm.heartbeat().unwrap();
    let commit = vm
        .atomic_group_end(ActionOptions::named("commit"), true)
        .unwrap()
        .unwrap();
    // The attempt reads what the group wrote, once it is sent
    let head = vm
        .cmd_state_get(StateScope::Object, "head".to_owned(), Default::default())
        .unwrap();
    assert_eq!(take_all(&mut vm), vec![notification(head, success("h"))]);

    notify(&mut vm, result(7, invocation_id("inv_expire")));
    notify(&mut vm, result(8, void()));
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(
                send.invocation_id_notification_handle,
                Value::InvocationId("inv_expire".to_owned())
            ),
            notification(commit, Value::Void),
        ]
    );

    let mut output = Output::of(&mut vm);
    output.next::<HeartbeatMessage>();
    let group = output.next::<AtomicGroupActionMessage>();
    // The 02 §14 flow: the send's invocation id is result 7, the commit is result 8
    assert_eq!(group.result_id, 8);
    assert_eq!(group.name, "commit");
    assert_that!(
        group
            .actions
            .into_iter()
            .map(|a| a.action.unwrap())
            .collect::<Vec<_>>(),
        elements_are![
            matches_pattern!(Grouped::SetState(pat!(SetStateActionMessage {
                key: eq(Bytes::from_static(b"tail")),
                result_id: eq(0)
            }))),
            matches_pattern!(Grouped::SetState(pat!(SetStateActionMessage {
                key: eq(Bytes::from_static(b"head"))
            }))),
            matches_pattern!(Grouped::Call(pat!(CallActionMessage {
                invocation_id_result_id: eq(7),
                result_id: eq(0)
            }))),
        ]
    );
    assert!(output.is_over());
}

#[test]
fn what_a_group_cannot_hold_fails_the_attempt() {
    for not_groupable in [
        (|vm: &mut CoreExplicitRecoveryVM| {
            vm.cmd_state_get(StateScope::Object, "k".to_owned(), Default::default())
                .map(drop)
        }) as fn(&mut CoreExplicitRecoveryVM) -> std::result::Result<(), Error>,
        |vm| vm.cmd_flush(Default::default()).map(drop),
        |vm| {
            vm.cmd_signal_read("s".to_owned(), 0, Default::default())
                .map(drop)
        },
        |vm| vm.atomic_group_start(),
        |vm| vm.end(NonEmptyValue::Success(Bytes::new())),
    ] {
        let mut vm = running(start_message(1));
        vm.atomic_group_start().unwrap();
        set(&mut vm, StateScope::Object, "head", "h");
        assert_that!(not_groupable(&mut vm), err(anything()));
        assert_eq!(vm.state(), AttemptState::Closed);

        // Nothing of the group left
        let mut output = Output::of(&mut vm);
        assert_eq!(
            output.next::<ErrorMessage>().code,
            u32::from(codes::INTERNAL)
        );
        assert!(output.is_over());
    }
}

#[test]
fn an_aborted_group_sends_nothing_and_changes_nothing() {
    let mut vm = running(StartMessage {
        is_object_scope_partial_state: false,
        ..start_message(1)
    });
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "head", "h");
    let send = vm
        .cmd_send(target(), Bytes::new(), None, Default::default())
        .unwrap();
    assert_eq!(
        vm.atomic_group_abort().unwrap(),
        vec![send.invocation_id_notification_handle]
    );
    // The write never happened
    let head = vm
        .cmd_state_get(StateScope::Object, "head".to_owned(), Default::default())
        .unwrap();
    assert_eq!(take_all(&mut vm), vec![notification(head, Value::Void)]);
    // The id the send took is not used again
    vm.cmd_flush(Default::default()).unwrap();

    let mut output = Output::of(&mut vm);
    assert_eq!(output.next::<FlushActionMessage>().result_id, 2);
    assert!(output.is_over());
}

// --- Shutdown

#[test]
fn shutdown_answers_at_once_with_the_notifications_handed_out() {
    let mut vm = running(start_message(1));
    let call = vm
        .cmd_call(target(), Bytes::new(), None, Default::default())
        .unwrap();
    notify(&mut vm, result(1, invocation_id("inv_callee")));
    assert_eq!(
        take_all(&mut vm),
        vec![notification(
            call.invocation_id_notification_handle,
            Value::InvocationId("inv_callee".to_owned())
        )]
    );
    // Received and decoded, but the handler never got it
    notify(&mut vm, result(2, value("returned")));
    // An action the handler already made goes before the answer
    set(&mut vm, StateScope::Invocation, "progress", "1");

    vm.notify_input_closed();

    assert_eq!(vm.state(), AttemptState::Closed);
    assert_that!(vm.take_next_notification(), err(is_shut_down()));
    assert_that!(vm.cmd_flush(Default::default()), err(is_shut_down()));
    assert_that!(
        vm.end(NonEmptyValue::Success(Bytes::new())),
        err(is_shut_down())
    );
    vm.notify_error(Error::internal("too late"));

    let mut output = Output::of(&mut vm);
    output.next::<CallActionMessage>();
    output.next::<SetStateActionMessage>();
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 1
        }
    );
    assert!(output.is_over());
}

#[test]
fn shutdown_drops_an_open_group() {
    let mut vm = running(start_message(1));
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "head", "h");
    vm.notify_input_closed();

    let mut output = Output::of(&mut vm);
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 0
        }
    );
    assert!(output.is_over());
}

// --- Cancellation

#[test]
fn the_cancel_signal_is_a_notification_and_counts_once_taken() {
    let mut vm = running(start_message(1));
    assert!(!vm.is_cancelled());
    notify(
        &mut vm,
        SignalNotificationMessage {
            signal_id: BuiltInSignal::Cancel as i32,
        },
    );
    // Not told to the handler yet
    assert!(!vm.is_cancelled());
    assert_eq!(
        take_all(&mut vm),
        vec![notification(CANCEL_NOTIFICATION_HANDLE, Value::Void)]
    );
    assert!(vm.is_cancelled());
    vm.notify_input_closed();

    let mut output = Output::of(&mut vm);
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 1
        }
    );
    assert!(output.is_over());

    // Cancelled before the attempt: known from the start, and no notification
    let mut vm = running(StartMessage {
        cancelled: true,
        ..start_message(1)
    });
    assert!(vm.is_cancelled());
    assert_eq!(take_all(&mut vm), vec![]);
}

// --- Signals, flush, spans, record levels

#[test]
fn signals_flush_spans_and_record_levels() {
    let mut vm = running(start_message(1));
    let turn = vm.span_start("turn".to_owned(), None).unwrap();
    let model = vm.span_start("model".to_owned(), Some(turn)).unwrap();
    let approval = vm
        .cmd_signal_read(
            "approval".to_owned(),
            2,
            ActionOptions::named("wait for approval").inside(model),
        )
        .unwrap();
    vm.cmd_state_set(
        StateScope::Object,
        "v22/tail/1".to_owned(),
        Bytes::from_static(b"page"),
        ActionOptions::default().recorded(RecordLevel::None),
        false,
    )
    .unwrap();
    vm.span_end(
        model,
        Some(NonEmptyValue::Success(Bytes::from_static(b"42 tokens"))),
    )
    .unwrap();
    let flushed = vm.cmd_flush(Default::default()).unwrap();
    notify(&mut vm, result(2, void()));
    notify(&mut vm, result(1, value("yes")));
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(flushed, Value::Void),
            notification(approval, success("yes"))
        ]
    );
    // Ending a span twice fails the attempt
    assert_that!(vm.span_end(model, None), err(anything()));

    let mut output = Output::of(&mut vm);
    assert_that!(
        output.next::<SpanStartActionMessage>(),
        pat!(SpanStartActionMessage {
            span_id: eq(1),
            name: eq("turn"),
            parent_span_id: eq(0)
        })
    );
    assert_that!(
        output.next::<SpanStartActionMessage>(),
        pat!(SpanStartActionMessage {
            span_id: eq(2),
            name: eq("model"),
            parent_span_id: eq(1)
        })
    );
    assert_that!(
        output.next::<ReadSignalActionMessage>(),
        pat!(ReadSignalActionMessage {
            signal_name: eq("approval"),
            signal_index: eq(2),
            result_id: eq(1),
            name: eq("wait for approval"),
            parent_span_id: eq(2)
        })
    );
    assert_eq!(
        output.next::<SetStateActionMessage>().record,
        messages::RecordLevel::RecordNone as i32
    );
    assert_eq!(output.next::<SpanEndActionMessage>().span_id, 2);
    assert_eq!(output.next::<FlushActionMessage>().result_id, 2);
    output.next::<ErrorMessage>();
    assert!(output.is_over());
}

// --- Features

#[test]
fn a_missing_feature_is_named_in_the_error_metadata() {
    let mut vm = running(StartMessage {
        features: vec![7],
        ..start_message(1)
    });
    assert!(vm.is_feature_supported(7));
    assert!(!vm.is_feature_supported(3));
    vm.notify_error(missing_features(&[3, 9]));

    let mut output = Output::of(&mut vm);
    assert_that!(
        output.next::<ErrorMessage>(),
        pat!(ErrorMessage {
            code: eq(573),
            metadata: eq(vec![FailureMetadata {
                key: "restate.missing_features".to_owned(),
                value: "3,9".to_owned()
            }])
        })
    );
    assert!(output.is_over());
}

// --- Protocol violations: 571

fn assert_violation(vm: &mut CoreExplicitRecoveryVM) {
    assert_eq!(vm.state(), AttemptState::Closed);
    let mut output = Output::of(vm);
    let mut last = None;
    while let Some(raw) = output.0.consume_next().unwrap() {
        last = Some(raw);
    }
    let last = last.expect("an error message");
    assert_eq!(last.raw_header().ty(), MessageType::Error);
    assert_eq!(
        ErrorMessage::decode(last.into_payload()).unwrap().code,
        u32::from(codes::PROTOCOL_VIOLATION)
    );
}

#[test]
fn what_the_runtime_must_not_send_is_a_violation() {
    // A message type this version does not define: the durable execution protocol's input command
    let mut vm = running(start_message(1));
    vm.notify_input(Bytes::from_static(&[0x04, 0x00, 0, 0, 0, 0, 0, 0]));
    assert_violation(&mut vm);

    // A reserved bit of the header
    let mut vm = running(start_message(1));
    vm.notify_input(Bytes::from_static(&[0x80, 0x01, 0x80, 0, 0, 0, 0, 0]));
    assert_violation(&mut vm);

    // A second start
    let mut vm = running(start_message(1));
    notify(&mut vm, start_message(1));
    assert_violation(&mut vm);

    // A message only the SDK sends
    let mut vm = running(start_message(1));
    notify(&mut vm, HeartbeatMessage {});
    assert_violation(&mut vm);

    // A result nothing waits for, and one that comes twice
    let mut vm = running(start_message(5));
    notify(&mut vm, result(9, void()));
    assert_violation(&mut vm);
    let mut vm = running(start_message(5));
    vm.cmd_flush(Default::default()).unwrap();
    notify(&mut vm, result(5, void()));
    notify(&mut vm, result(5, void()));
    assert_violation(&mut vm);

    // A result of a variant its action does not allow
    let mut vm = running(start_message(5));
    vm.cmd_flush(Default::default()).unwrap();
    notify(&mut vm, result(5, value("not void")));
    assert_violation(&mut vm);

    // The stream closed before the start
    let mut vm = new_vm();
    vm.notify_input_closed();
    assert_violation(&mut vm);
}

#[test]
fn more_of_what_the_runtime_must_not_send() {
    // A result of an earlier attempt's action: the runtime drops those (G7)
    let mut vm = running(start_message(5));
    notify(&mut vm, result(3, value("late")));
    assert_violation(&mut vm);

    // A result id of 0
    let mut vm = running(start_message(5));
    notify(&mut vm, result(0, void()));
    assert_violation(&mut vm);

    // A notification before the start
    let mut vm = new_vm();
    notify(&mut vm, result(1, void()));
    assert_violation(&mut vm);

    // A preloaded entry of no scope, or of a scope this version does not define
    for scope in [messages::StateScope::Unspecified as i32, 7] {
        let mut vm = new_vm();
        notify(
            &mut vm,
            StartMessage {
                state_map: vec![StateEntry {
                    scope,
                    key: Bytes::from_static(b"k"),
                    value: None,
                }],
                ..start_message(1)
            },
        );
        assert_violation(&mut vm);
    }

    // A built-in signal this version does not define
    let mut vm = running(start_message(1));
    notify(&mut vm, SignalNotificationMessage { signal_id: 2 });
    assert_violation(&mut vm);

    // The cancel signal twice, or after a start that said cancelled: an attempt learns of it once
    let mut vm = running(start_message(1));
    let cancel = SignalNotificationMessage {
        signal_id: BuiltInSignal::Cancel as i32,
    };
    notify(&mut vm, cancel);
    notify(&mut vm, cancel);
    assert_violation(&mut vm);
    let mut vm = running(StartMessage {
        cancelled: true,
        ..start_message(1)
    });
    notify(&mut vm, cancel);
    assert_violation(&mut vm);

    // A call's invocation id that is not one: nothing set reads as void, which it does not allow
    let mut vm = running(start_message(1));
    vm.cmd_send(target(), Bytes::new(), None, Default::default())
        .unwrap();
    notify(
        &mut vm,
        ResultNotificationMessage {
            result_id: 1,
            result: None,
        },
    );
    assert_violation(&mut vm);

    // A message that cannot be decoded
    let mut vm = running(start_message(1));
    vm.notify_input(Bytes::from_static(&[
        0x80, 0x00, 0, 0, 0, 0, 0, 2, 0xff, 0xff,
    ]));
    assert_violation(&mut vm);
}

#[test]
fn a_result_with_nothing_set_reads_as_void() {
    let mut vm = running(start_message(1));
    let flushed = vm.cmd_flush(Default::default()).unwrap();
    let read = vm
        .cmd_signal_read("s".to_owned(), 0, Default::default())
        .unwrap();
    for result_id in [1, 2] {
        notify(
            &mut vm,
            ResultNotificationMessage {
                result_id,
                result: None,
            },
        );
    }
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(flushed, Value::Void),
            notification(read, Value::Void)
        ]
    );
    assert_eq!(vm.state(), AttemptState::Running);
}

// --- More of what the plan claims

#[test]
fn an_empty_group_whose_result_is_wanted_is_a_flush() {
    let mut vm = running(start_message(4));
    vm.atomic_group_start().unwrap();
    let commit = vm
        .atomic_group_end(Default::default(), true)
        .unwrap()
        .unwrap();
    // And one whose result nobody wants sends nothing
    vm.atomic_group_start().unwrap();
    assert_eq!(
        vm.atomic_group_end(Default::default(), false).unwrap(),
        None
    );
    notify(&mut vm, result(4, void()));
    assert_eq!(take_all(&mut vm), vec![notification(commit, Value::Void)]);

    let mut output = Output::of(&mut vm);
    assert_eq!(output.next::<FlushActionMessage>().result_id, 4);
    assert!(output.is_over());
}

#[test]
fn what_the_vm_refuses_from_the_sdk() {
    // Object state where the target has no key
    let mut vm = running(StartMessage {
        key: String::new(),
        ..start_message(1)
    });
    // Invocation state is there for every handler
    set(&mut vm, StateScope::Invocation, "cursor", "1");
    assert_that!(
        vm.cmd_state_get(StateScope::Object, "k".to_owned(), Default::default()),
        err(anything())
    );
    assert_eq!(vm.state(), AttemptState::Closed);

    // A parent span this attempt never started
    let mut vm = running(start_message(1));
    let never_started = crate::explicit_recovery::SpanId::try_from(9).unwrap();
    assert_that!(
        vm.cmd_flush(ActionOptions::default().inside(never_started)),
        err(anything())
    );

    // A span that ended can still be a parent
    let mut vm = running(start_message(1));
    let ended = vm.span_start("turn".to_owned(), None).unwrap();
    vm.span_end(ended, None).unwrap();
    vm.cmd_flush(ActionOptions::default().inside(ended))
        .unwrap();
}

// --- Invalid input (D65): the action fails alone, the attempt goes on

#[test]
fn invalid_input_fails_the_action_alone_sends_nothing_and_takes_no_result_id() {
    let mut vm = running(start_message(1));
    let options = ActionOptions::default;
    for malformed in [
        "",
        "inv_garbage",
        "inv_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "dp_1x",
    ] {
        assert_that!(
            vm.cmd_cancel_invocation(malformed.to_owned(), options(), true),
            err(is_invalid_input())
        );
        assert_that!(
            vm.cmd_signal_send(
                malformed.to_owned(),
                "go".to_owned(),
                None,
                options(),
                false
            ),
            err(is_invalid_input())
        );
        assert_that!(
            vm.cmd_attach_invocation(
                AttachInvocationTarget::InvocationId(malformed.to_owned()),
                options()
            ),
            err(is_invalid_input())
        );
    }
    let error = vm
        .cmd_cancel_invocation("inv_garbage".to_owned(), options(), false)
        .unwrap_err();
    assert_that!(
        error.message(),
        contains_substring("'inv_garbage' is not an invocation id")
    );

    // Scopes of an attach's target, and the target of a call or a send
    for scope in ["", "a/b", &"s".repeat(37)] {
        assert_that!(
            vm.cmd_attach_invocation(
                AttachInvocationTarget::WorkflowId {
                    name: "Wf".to_owned(),
                    key: "k".to_owned(),
                    scope: Some(scope.to_owned())
                },
                options()
            ),
            err(is_invalid_input())
        );
        assert_that!(
            vm.cmd_attach_invocation(
                AttachInvocationTarget::IdempotencyId {
                    service_name: "Svc".to_owned(),
                    service_key: None,
                    handler_name: "h".to_owned(),
                    idempotency_key: "i".to_owned(),
                    scope: Some(scope.to_owned())
                },
                options()
            ),
            err(is_invalid_input())
        );
        assert_that!(
            vm.cmd_call(
                Target {
                    scope: Some(scope.to_owned()),
                    ..target()
                },
                Bytes::new(),
                None,
                options()
            ),
            err(is_invalid_input())
        );
    }
    for bad in [
        (|| Target {
            idempotency_key: Some(String::new()),
            ..target()
        }) as fn() -> Target,
        || Target {
            limit_key: Some(String::new()),
            scope: Some("s".to_owned()),
            ..target()
        },
        // A limit key without a scope
        || Target {
            limit_key: Some("l".to_owned()),
            ..target()
        },
    ] {
        assert_that!(
            vm.cmd_send(bad(), Bytes::new(), None, options()),
            err(is_invalid_input())
        );
        assert_that!(
            vm.cmd_call(bad(), Bytes::new(), None, options()),
            err(is_invalid_input())
        );
    }

    // The attempt goes on: nothing was sent, and the next action takes result id 1
    assert_eq!(vm.state(), AttemptState::Running);
    assert!(vm.take_output().is_empty());
    let cancelled = vm
        .cmd_cancel_invocation(OTHER.to_owned(), options(), true)
        .unwrap()
        .unwrap();
    let attached = vm
        .cmd_attach_invocation(
            AttachInvocationTarget::InvocationId(OTHER.to_owned()),
            options(),
        )
        .unwrap();
    vm.cmd_signal_send(OTHER.to_owned(), "go".to_owned(), None, options(), false)
        .unwrap();
    vm.cmd_send(
        Target {
            scope: Some("tenant-1".to_owned()),
            limit_key: Some("l".to_owned()),
            ..target()
        },
        Bytes::new(),
        None,
        options(),
    )
    .unwrap();
    notify(&mut vm, result(1, void()));
    notify(&mut vm, result(2, value("out")));
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(cancelled, Value::Void),
            notification(attached, success("out"))
        ]
    );
    let mut output = Output::of(&mut vm);
    assert_that!(
        output.next::<messages::CancelInvocationActionMessage>(),
        pat!(messages::CancelInvocationActionMessage {
            invocation_id: eq(OTHER),
            result_id: eq(1)
        })
    );
    assert_eq!(
        output
            .next::<messages::AttachInvocationActionMessage>()
            .result_id,
        2
    );
    assert_eq!(
        output
            .next::<messages::SendSignalActionMessage>()
            .target_invocation_id,
        OTHER
    );
    assert_eq!(
        output.next::<CallActionMessage>().invocation_id_result_id,
        3
    );
    assert!(output.is_over());
}

#[test]
fn invalid_input_in_a_group_leaves_it_open_for_the_sdk_to_abort() {
    let mut vm = running(start_message(1));
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "head", "h");
    assert_that!(
        vm.cmd_signal_send(
            "inv_bogus".to_owned(),
            "go".to_owned(),
            None,
            Default::default(),
            true
        ),
        err(is_invalid_input())
    );
    // Still in the group: more can go in, and the SDK aborts it, as on every error in it
    set(&mut vm, StateScope::Object, "tail", "t");
    assert_eq!(vm.atomic_group_abort().unwrap(), vec![]);
    assert_eq!(vm.state(), AttemptState::Running);
    // The group, retried with a valid id, goes out with result ids from 1
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "head", "h");
    vm.cmd_signal_send(
        OTHER.to_owned(),
        "go".to_owned(),
        None,
        Default::default(),
        true,
    )
    .unwrap()
    .unwrap();
    vm.atomic_group_end(Default::default(), false).unwrap();
    let mut output = Output::of(&mut vm);
    let group = output.next::<AtomicGroupActionMessage>();
    assert_eq!(group.actions.len(), 2);
    match &group.actions[1].action {
        Some(Grouped::SendSignal(a)) => assert_eq!(a.result_id, 1),
        other => panic!("unexpected {other:?}"),
    }
    assert!(output.is_over());

    // An attach cannot be in a group at all: that misuse still fails the attempt
    let mut vm = running(start_message(1));
    vm.atomic_group_start().unwrap();
    assert_that!(
        vm.cmd_attach_invocation(
            AttachInvocationTarget::InvocationId("inv_bogus".to_owned()),
            Default::default()
        ),
        err(predicate(|e: &Error| !e.is_invalid_input()))
    );
    assert_eq!(vm.state(), AttemptState::Closed);
}

#[test]
fn invalid_input_once_the_attempt_is_over_says_why_it_is_over() {
    // Shut down: the shutdown, not the input
    let mut vm = running(start_message(1));
    vm.notify_input_closed();
    assert_that!(
        vm.cmd_cancel_invocation("inv_bogus".to_owned(), Default::default(), false),
        err(is_shut_down())
    );
    // Before the start: the VM's misuse, which is no invalid input either
    let mut vm = new_vm();
    assert_that!(
        vm.cmd_cancel_invocation("inv_bogus".to_owned(), Default::default(), false),
        err(predicate(|e: &Error| !e.is_invalid_input()))
    );
}

#[test]
fn a_span_started_in_a_group_can_parent_what_follows_in_it() {
    let mut vm = running(start_message(1));
    vm.atomic_group_start().unwrap();
    let span = vm.span_start("commit".to_owned(), None).unwrap();
    vm.cmd_state_set(
        StateScope::Object,
        "head".to_owned(),
        Bytes::from_static(b"h"),
        ActionOptions::default().inside(span),
        false,
    )
    .unwrap();
    vm.span_end(span, None).unwrap();
    vm.atomic_group_end(Default::default(), false).unwrap();

    let mut output = Output::of(&mut vm);
    let group = output.next::<AtomicGroupActionMessage>();
    assert_eq!(group.result_id, 0);
    assert_eq!(group.actions.len(), 3);
    assert!(output.is_over());
}

#[test]
fn ids_follow_the_wire_order_in_a_group_and_a_call_in_it_may_want_its_result() {
    let mut vm = running(start_message(10));
    vm.atomic_group_start().unwrap();
    // 10: a write whose result is wanted
    let written = vm
        .cmd_state_set(
            StateScope::Invocation,
            "step".to_owned(),
            Bytes::from_static(b"1"),
            Default::default(),
            true,
        )
        .unwrap()
        .unwrap();
    // 11 and 12: a call in a group that waits for what the callee returns
    let call = vm
        .cmd_call(target(), Bytes::new(), None, Default::default())
        .unwrap();
    // 13: a cancel, 14: a signal sent; the group's own last, 15
    let cancelled = vm
        .cmd_cancel_invocation(OTHER.to_owned(), Default::default(), true)
        .unwrap()
        .unwrap();
    vm.cmd_signal_send(
        OTHER.to_owned(),
        "go".to_owned(),
        None,
        Default::default(),
        true,
    )
    .unwrap()
    .unwrap();
    let commit = vm
        .atomic_group_end(Default::default(), true)
        .unwrap()
        .unwrap();

    notify(&mut vm, result(10, void()));
    notify(&mut vm, result(11, invocation_id("inv_callee")));
    notify(&mut vm, result(13, void()));
    notify(&mut vm, result(14, void()));
    notify(&mut vm, result(15, void()));
    notify(&mut vm, result(12, value("returned")));
    let taken: Vec<_> = take_all(&mut vm).into_iter().map(|n| n.handle).collect();
    assert_eq!(taken.len(), 6);
    assert_eq!(taken[0], written);
    assert_eq!(taken[1], call.invocation_id_notification_handle);
    assert_eq!(taken[2], cancelled);
    assert_eq!(taken[4], commit);
    assert_eq!(taken[5], call.call_notification_handle);

    let mut output = Output::of(&mut vm);
    let group = output.next::<AtomicGroupActionMessage>();
    assert_eq!(group.result_id, 15);
    let ids: Vec<_> = group
        .actions
        .into_iter()
        .map(|a| match a.action.unwrap() {
            Grouped::SetState(a) => vec![a.result_id],
            Grouped::Call(a) => vec![a.invocation_id_result_id, a.result_id],
            Grouped::CancelInvocation(a) => vec![a.result_id],
            Grouped::SendSignal(a) => vec![a.result_id],
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![vec![10], vec![11, 12], vec![13], vec![14]]);
    assert!(output.is_over());
}

#[test]
fn the_sdk_closes_after_the_runtime() {
    // EndMessage: the runtime closes first, then the SDK
    let mut vm = running(start_message(1));
    assert!(!vm.should_close_output());
    vm.end(NonEmptyValue::Success(Bytes::from_static(b"out")))
        .unwrap();
    assert_eq!(vm.state(), AttemptState::Closed);
    assert!(!vm.should_close_output());
    // What the runtime sends meanwhile is ignored
    notify(&mut vm, result(1, void()));
    vm.notify_input_closed();
    assert!(vm.should_close_output());
    let mut output = Output::of(&mut vm);
    assert_that!(
        output.next::<EndMessage>(),
        pat!(EndMessage {
            output: some(eq(end_message::Output::Value(
                Bytes::from_static(b"out").into()
            )))
        })
    );
    assert!(output.is_over());

    // ErrorMessage: the same
    let mut vm = running(start_message(1));
    vm.notify_error(Error::internal("boom"));
    assert!(!vm.should_close_output());
    vm.notify_input_closed();
    assert!(vm.should_close_output());

    // A shutdown: the runtime closed first, and the SDK answers and closes at once
    let mut vm = running(start_message(1));
    vm.notify_input_closed();
    assert!(vm.should_close_output());
}

#[test]
fn an_unasked_shutdown_says_what_was_processed_and_drops_an_open_group() {
    let mut vm = running(start_message(1));
    vm.cmd_flush(Default::default()).unwrap();
    notify(&mut vm, result(1, void()));
    take_all(&mut vm);
    vm.atomic_group_start().unwrap();
    set(&mut vm, StateScope::Object, "head", "h");
    vm.shutdown();
    assert_eq!(vm.state(), AttemptState::Closed);
    assert_that!(vm.cmd_flush(Default::default()), err(is_shut_down()));
    // The runtime closes its side after the last message
    assert!(!vm.should_close_output());
    vm.notify_input_closed();
    assert!(vm.should_close_output());
    // Nothing, once over
    vm.shutdown();

    let mut output = Output::of(&mut vm);
    output.next::<FlushActionMessage>();
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 1
        }
    );
    assert!(output.is_over());
}

#[test]
fn a_cancel_signal_not_taken_is_not_counted() {
    let mut vm = running(start_message(1));
    let read = vm
        .cmd_signal_read("approval".to_owned(), 0, Default::default())
        .unwrap();
    notify(&mut vm, result(1, value("yes")));
    notify(
        &mut vm,
        SignalNotificationMessage {
            signal_id: BuiltInSignal::Cancel as i32,
        },
    );
    // In arrival order: the result first; the handler took only that one
    assert_eq!(
        vm.take_next_notification().unwrap(),
        Some(notification(read, success("yes")))
    );
    assert!(!vm.is_cancelled());
    vm.notify_input_closed();
    let mut output = Output::of(&mut vm);
    output.next::<ReadSignalActionMessage>();
    assert_eq!(
        output.next::<ShutdownMessage>(),
        ShutdownMessage {
            processed_notifications: 1
        }
    );
}

#[test]
fn clear_all_makes_a_partial_scope_whole_and_aborted_ids_are_skipped() {
    let mut vm = running(start_message(1));
    // The invocation scope is partial in start_message: a read is asked
    vm.cmd_state_clear_all(StateScope::Invocation, Default::default(), false)
        .unwrap();
    let missing = vm
        .cmd_state_get(StateScope::Invocation, "k".to_owned(), Default::default())
        .unwrap();
    let keys = vm
        .cmd_state_get_keys(StateScope::Invocation, Default::default())
        .unwrap();
    assert_eq!(
        take_all(&mut vm),
        vec![
            notification(missing, Value::Void),
            notification(keys, Value::StateKeys(vec![]))
        ]
    );
    // The object scope is untouched: still asked
    vm.cmd_state_get(StateScope::Object, "k".to_owned(), Default::default())
        .unwrap();

    vm.atomic_group_start().unwrap();
    vm.cmd_call(target(), Bytes::new(), None, Default::default())
        .unwrap();
    vm.atomic_group_abort().unwrap();
    vm.cmd_flush(Default::default()).unwrap();

    let mut output = Output::of(&mut vm);
    output.next::<ClearAllStateActionMessage>();
    assert_eq!(output.next::<GetStateActionMessage>().result_id, 1);
    // 2 and 3 went to the aborted call
    assert_eq!(output.next::<FlushActionMessage>().result_id, 4);
    assert!(output.is_over());
}
