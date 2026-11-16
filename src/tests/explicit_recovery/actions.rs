//! The actions of a running attempt, and their results.

use super::*;
use test_log::test;

use crate::explicit_recovery::TraceId;
use crate::service_protocol::messages::start_message::StateEntry;
use crate::service_protocol::messages::{
    attach_invocation_command_message, attach_invocation_completion_notification_message,
    call_completion_notification_message, get_lazy_state_completion_notification_message,
    get_state_ephemeral_notification_message, propose_run_completion_message,
    run_completion_notification_message, send_signal_command_message,
    AttachInvocationCommandMessage, AttachInvocationCompletionNotificationMessage,
    AwaitingOnMessage, CallCommandMessage, CallCompletionNotificationMessage,
    CallInvocationIdCompletionNotificationMessage, ClearAllStateCommandMessage,
    ClearStateCommandMessage, CombinatorType, CommandAckMessage, Failure, Future,
    GetLazyStateCommandMessage, GetLazyStateCompletionNotificationMessage,
    GetLazyStateKeysCommandMessage, GetLazyStateKeysCompletionNotificationMessage,
    GetStateEphemeralCommandMessage, GetStateEphemeralNotificationMessage,
    OneWayCallCommandMessage, ProposeRunCompletionAckMessage, ProposeRunCompletionMessage,
    RunCommandMessage, RunCompletionNotificationMessage, SendSignalCommandMessage, StateKeys,
};
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::{AttachInvocationTarget, TerminalFailure};
use std::time::Duration;

fn start_with_state(partial_state: bool, state: &[(&'static str, &'static str)]) -> StartMessage {
    StartMessage {
        partial_state,
        state_map: state
            .iter()
            .map(|(k, v)| StateEntry {
                key: Bytes::from_static(k.as_bytes()),
                value: Bytes::from_static(v.as_bytes()),
            })
            .collect(),
        ..start_message(1)
    }
}

fn running(start: StartMessage) -> ErTestCase {
    ErTestCase::new()
        .input(start)
        .input(input_entry_message(b"my-input"))
}

fn get(vm: &mut CoreExplicitRecoveryVM, key: &str) -> NotificationHandle {
    vm.cmd_state_get(key.to_owned(), None).unwrap()
}

/// Without an ack, a fire-and-forget action's handle is notified at once.
fn notified_at_once(vm: &mut CoreExplicitRecoveryVM, handle: NotificationHandle) {
    assert_eq!(
        vm.take_next_notification().unwrap(),
        Some(notification(handle, Value::Void))
    );
}

fn set(vm: &mut CoreExplicitRecoveryVM, key: &str, value: &'static str) {
    let handle = vm
        .cmd_state_set(
            key.to_owned(),
            Bytes::from_static(value.as_bytes()),
            None,
            false,
        )
        .unwrap();
    notified_at_once(vm, handle);
}

fn call(vm: &mut CoreExplicitRecoveryVM) -> CallHandle {
    vm.cmd_call(
        target(),
        Bytes::from_static(b"my-param"),
        Default::default(),
    )
    .unwrap()
}

fn call_result(completion_id: u32, value: &'static str) -> CallCompletionNotificationMessage {
    CallCompletionNotificationMessage {
        completion_id,
        result: Some(call_completion_notification_message::Result::Value(
            Bytes::from_static(value.as_bytes()).into(),
        )),
    }
}

// --- State

#[test]
fn state_sent_at_the_start_is_answered_locally() {
    let mut output = running(start_with_state(false, &[("my-key", "my-value")])).run(|vm, _| {
        vm.get_input().unwrap();
        let present = get(vm, "my-key");
        let absent = get(vm, "other-key");
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(present, success("my-value")),
                notification(absent, Value::Void)
            ]
        );
        end_with(vm, "my-output");
    });

    // No command for the reads
    assert_end(&mut output, "my-output");
}

#[test]
fn writes_of_the_attempt_are_read_locally() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        set(vm, "my-key", "my-value");
        let after_set = get(vm, "my-key");
        let cleared = vm
            .cmd_state_clear("my-key".to_owned(), None, false)
            .unwrap();
        let after_clear = get(vm, "my-key");
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(after_set, success("my-value")),
                notification(cleared, Value::Void),
                notification(after_clear, Value::Void)
            ]
        );
        end_with(vm, "my-output");
    });

    assert_eq!(
        output.next_decoded::<SetStateCommandMessage>().unwrap(),
        SetStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            value: Some(Bytes::from_static(b"my-value").into()),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<ClearStateCommandMessage>().unwrap(),
        ClearStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            ..Default::default()
        }
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn unknown_state_is_read_ephemerally_on_v8() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let first = get(vm, "first-key");
        let second = get(vm, "second-key");
        notify(
            vm,
            encoder,
            GetStateEphemeralNotificationMessage {
                ephemeral_completion_id: 2,
                result: Some(get_state_ephemeral_notification_message::Result::Void(
                    Default::default(),
                )),
            },
        );
        notify(
            vm,
            encoder,
            GetStateEphemeralNotificationMessage {
                ephemeral_completion_id: 1,
                result: Some(get_state_ephemeral_notification_message::Result::Value(
                    Bytes::from_static(b"my-value").into(),
                )),
            },
        );
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(second, Value::Void),
                notification(first, success("my-value"))
            ]
        );
        end_with(vm, "my-output");
    });

    for (id, key) in [(1, "first-key"), (2, "second-key")] {
        assert_eq!(
            output
                .next_decoded::<GetStateEphemeralCommandMessage>()
                .unwrap(),
            GetStateEphemeralCommandMessage {
                ephemeral_completion_id: id,
                key: Bytes::copy_from_slice(key.as_bytes()),
            }
        );
    }
    assert_end(&mut output, "my-output");
}

#[test]
fn unknown_state_is_read_lazily_on_v7() {
    let mut output = ErTestCase::with_version(Version::V7)
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, encoder| {
            vm.get_input().unwrap();
            let handle = vm
                .cmd_state_get("my-key".to_owned(), Some("my-key".to_owned()))
                .unwrap();
            notify(
                vm,
                encoder,
                GetLazyStateCompletionNotificationMessage {
                    completion_id: 1,
                    result: Some(
                        get_lazy_state_completion_notification_message::Result::Value(
                            Bytes::from_static(b"my-value").into(),
                        ),
                    ),
                },
            );
            assert_eq!(
                take_notifications(vm),
                vec![notification(handle, success("my-value"))]
            );
            end_with(vm, "my-output");
        });

    assert_eq!(
        output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
        GetLazyStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            result_completion_id: 1,
            name: "my-key".to_owned(),
        }
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn local_state_keys_leave_out_cleared_keys() {
    let mut output = running(start_with_state(false, &[("a", "1"), ("b", "2")])).run(|vm, _| {
        vm.get_input().unwrap();
        let cleared = vm.cmd_state_clear("b".to_owned(), None, false).unwrap();
        notified_at_once(vm, cleared);
        set(vm, "c", "3");
        let keys = vm.cmd_state_get_keys(None).unwrap();
        assert_eq!(
            take_notifications(vm),
            vec![notification(
                keys,
                Value::StateKeys(vec!["a".to_owned(), "c".to_owned()])
            )]
        );
        end_with(vm, "my-output");
    });

    output.next_decoded::<ClearStateCommandMessage>().unwrap();
    output.next_decoded::<SetStateCommandMessage>().unwrap();
    assert_end(&mut output, "my-output");
}

#[test]
fn partial_state_keys_are_read_lazily() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let keys = vm.cmd_state_get_keys(None).unwrap();
        notify(
            vm,
            encoder,
            GetLazyStateKeysCompletionNotificationMessage {
                completion_id: 1,
                state_keys: Some(StateKeys {
                    keys: vec![Bytes::from_static(b"a")],
                }),
            },
        );
        assert_eq!(
            take_notifications(vm),
            vec![notification(keys, Value::StateKeys(vec!["a".to_owned()]))]
        );
        end_with(vm, "my-output");
    });

    assert_eq!(
        output
            .next_decoded::<GetLazyStateKeysCommandMessage>()
            .unwrap(),
        GetLazyStateKeysCommandMessage {
            result_completion_id: 1,
            ..Default::default()
        }
    );
    assert_end(&mut output, "my-output");
}

// --- Acks

#[test]
fn state_writes_can_ask_for_an_ack() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let set_ack = vm
            .cmd_state_set(
                "my-key".to_owned(),
                Bytes::from_static(b"my-value"),
                None,
                true,
            )
            .unwrap();
        // Index 2
        set(vm, "other-key", "other-value");
        let clear_ack = vm.cmd_state_clear("my-key".to_owned(), None, true).unwrap();

        notify(vm, encoder, CommandAckMessage { command_index: 1 });
        assert_eq!(
            take_notifications(vm),
            vec![notification(set_ack, Value::Void)]
        );
        // Not asked for, or already delivered
        notify(vm, encoder, CommandAckMessage { command_index: 2 });
        notify(vm, encoder, CommandAckMessage { command_index: 1 });
        assert_eq!(take_notifications(vm), vec![]);
        notify(vm, encoder, CommandAckMessage { command_index: 3 });
        assert_eq!(
            take_notifications(vm),
            vec![notification(clear_ack, Value::Void)]
        );
        end_with(vm, "my-output");
    });

    for requested_ack in [true, false] {
        let (header, _) = output
            .next_with_header_decoded::<SetStateCommandMessage>()
            .unwrap();
        assert_eq!(header.requested_ack(), Some(requested_ack));
    }
    let (header, _) = output
        .next_with_header_decoded::<ClearStateCommandMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    let (header, _) = output
        .next_with_header_decoded::<OutputCommandMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(false));
    assert_eq!(output.next_decoded::<EndMessage>().unwrap(), EndMessage {});
    assert_eq!(output.next(), None);
}

/// Every command takes an index, whether it asks for an ack or not: the runtime acks a command by
/// its index among all of them.
#[test]
fn every_command_counts_for_the_ack_index() {
    let mut output = ErTestCase::with_version(Version::V7)
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, encoder| {
            vm.get_input().unwrap();
            // Commands 1 to 4: a lazy get, a call, a trace's run, an unacked set
            vm.cmd_state_get("my-key".to_owned(), None).unwrap();
            vm.cmd_call(target(), Bytes::new(), None).unwrap();
            vm.trace_start("model".to_owned(), None).unwrap();
            set(vm, "other-key", "other-value");
            // Command 5
            let ack_handle = vm.cmd_state_clear("my-key".to_owned(), None, true).unwrap();

            notify(vm, encoder, CommandAckMessage { command_index: 4 });
            assert_eq!(take_notifications(vm), vec![]);
            notify(vm, encoder, CommandAckMessage { command_index: 5 });
            assert_eq!(
                take_notifications(vm),
                vec![notification(ack_handle, Value::Void)]
            );
            end_with(vm, "my-output");
        });

    output.next_decoded::<GetLazyStateCommandMessage>().unwrap();
    output.next_decoded::<CallCommandMessage>().unwrap();
    output.next_decoded::<RunCommandMessage>().unwrap();
    output.next_decoded::<SetStateCommandMessage>().unwrap();
    output.next_decoded::<ClearStateCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_end(&mut output, "my-output");
}

#[test]
fn cancel_invocation_can_ask_for_an_ack() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let ack_handle = vm
            .cmd_cancel_invocation("my-invocation".to_owned(), None, true)
            .unwrap();
        notify(vm, encoder, CommandAckMessage { command_index: 1 });
        assert_eq!(
            take_notifications(vm),
            vec![notification(ack_handle, Value::Void)]
        );
        end_with(vm, "my-output");
    });

    let (header, cancel) = output
        .next_with_header_decoded::<SendSignalCommandMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    assert_eq!(
        cancel,
        SendSignalCommandMessage {
            target_invocation_id: "my-invocation".to_owned(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        }
    );
    assert_end(&mut output, "my-output");
}

// --- Calls

#[test]
fn call_completes_with_the_invocation_id_and_the_result() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let handle = vm
            .cmd_call(
                Target {
                    key: Some("my-key".to_owned()),
                    idempotency_key: Some("my-idempotency-key".to_owned()),
                    ..target()
                },
                Bytes::from_static(b"my-param"),
                Some("my-call".to_owned()),
            )
            .unwrap();
        notify(
            vm,
            encoder,
            CallInvocationIdCompletionNotificationMessage {
                completion_id: 1,
                invocation_id: "my-invocation".to_owned(),
            },
        );
        notify(vm, encoder, call_result(2, "my-result"));
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(
                    handle.invocation_id_notification_handle,
                    Value::InvocationId("my-invocation".to_owned())
                ),
                notification(handle.call_notification_handle, success("my-result"))
            ]
        );
        end_with(vm, "my-output");
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            key: eq("my-key"),
            idempotency_key: some(eq("my-idempotency-key")),
            parameter: eq(Bytes::from_static(b"my-param")),
            invocation_id_notification_idx: eq(1),
            result_completion_id: eq(2),
            name: eq("my-call")
        })
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn call_completes_with_a_failure() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let handle = call(vm);
        notify(
            vm,
            encoder,
            CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Failure(
                    Failure {
                        code: 409,
                        message: "my-failure".to_owned(),
                        metadata: vec![],
                    },
                )),
            },
        );
        assert_eq!(
            take_notifications(vm),
            vec![notification(
                handle.call_notification_handle,
                Value::Failure(TerminalFailure {
                    code: 409,
                    message: "my-failure".to_owned(),
                    metadata: vec![],
                })
            )]
        );
        end_with(vm, "my-output");
    });

    output.next_decoded::<CallCommandMessage>().unwrap();
    assert_end(&mut output, "my-output");
}

#[test]
fn call_with_an_empty_idempotency_key_fails() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        assert_that!(
            vm.cmd_call(
                Target {
                    idempotency_key: Some("".to_owned()),
                    ..target()
                },
                Bytes::new(),
                Default::default()
            ),
            err(has_error_code(codes::INTERNAL))
        );
    });

    output.next_decoded::<ErrorMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn send_completes_with_the_invocation_id() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let handle = vm
            .cmd_send(
                target(),
                Bytes::from_static(b"my-param"),
                Some(Duration::from_millis(1234)),
                Default::default(),
            )
            .unwrap();
        notify(
            vm,
            encoder,
            CallInvocationIdCompletionNotificationMessage {
                completion_id: 1,
                invocation_id: "my-invocation".to_owned(),
            },
        );
        assert_eq!(
            take_notifications(vm),
            vec![notification(
                handle.invocation_id_notification_handle,
                Value::InvocationId("my-invocation".to_owned())
            )]
        );
        end_with(vm, "my-output");
    });

    assert_that!(
        output.next_decoded::<OneWayCallCommandMessage>().unwrap(),
        pat!(OneWayCallCommandMessage {
            service_name: eq("MySvc"),
            parameter: eq(Bytes::from_static(b"my-param")),
            invoke_time: eq(1234),
            invocation_id_notification_idx: eq(1)
        })
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn attach_completes_with_the_result() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let handle = vm
            .cmd_attach_invocation(
                AttachInvocationTarget::InvocationId("my-invocation".to_owned()),
                Default::default(),
            )
            .unwrap();
        notify(
            vm,
            encoder,
            AttachInvocationCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    attach_invocation_completion_notification_message::Result::Value(
                        Bytes::from_static(b"my-result").into(),
                    ),
                ),
            },
        );
        assert_eq!(
            take_notifications(vm),
            vec![notification(handle, success("my-result"))]
        );
        end_with(vm, "my-output");
    });

    assert_eq!(
        output
            .next_decoded::<AttachInvocationCommandMessage>()
            .unwrap(),
        AttachInvocationCommandMessage {
            target: Some(attach_invocation_command_message::Target::InvocationId(
                "my-invocation".to_owned()
            )),
            result_completion_id: 1,
            ..Default::default()
        }
    );
    assert_end(&mut output, "my-output");
}

// --- Traces

#[test]
fn trace_is_a_run_completed_by_a_proposal() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let trace = vm.trace_start("model".to_owned(), None).unwrap();
        vm.trace_end(trace, NonEmptyValue::Success(Bytes::from_static(b"tokens")))
            .unwrap();
        notify(
            vm,
            encoder,
            ProposeRunCompletionAckMessage { completion_id: 1 },
        );
        assert_eq!(vm.state(), AttemptState::Running);
        assert_eq!(take_notifications(vm), vec![]);
        end_with(vm, "my-output");
    });

    assert_eq!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        RunCommandMessage {
            result_completion_id: 1,
            name: "model".to_owned(),
        }
    );
    let (header, proposal) = output
        .next_with_header_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    assert_eq!(
        proposal,
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"tokens")
            )),
        }
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn the_run_completion_settles_a_trace_too() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        let trace = vm.trace_start("model".to_owned(), None).unwrap();
        vm.trace_end(trace, NonEmptyValue::Success(Bytes::from_static(b"tokens")))
            .unwrap();
        notify(
            vm,
            encoder,
            RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Value(
                    Bytes::from_static(b"tokens").into(),
                )),
            },
        );
        assert_eq!(vm.state(), AttemptState::Running);
        assert_eq!(take_notifications(vm), vec![]);
        end_with(vm, "my-output");
    });

    output.next_decoded::<RunCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_end(&mut output, "my-output");
}

#[test]
fn ending_a_trace_twice_fails() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        let trace = vm.trace_start("model".to_owned(), None).unwrap();
        let result = || NonEmptyValue::Success(Bytes::from_static(b"tokens"));
        vm.trace_end(trace, result()).unwrap();
        assert_that!(
            vm.trace_end(trace, result()),
            err(has_error_code(codes::INTERNAL))
        );
        assert_that!(
            vm.trace_end(TraceId::from(42), result()),
            err(has_error_code(codes::INTERNAL))
        );
    });

    output.next_decoded::<RunCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    output.next_decoded::<ErrorMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn acks_of_run_proposals_are_ignored() {
    let mut output = running(start_message(1)).run(|vm, encoder| {
        vm.get_input().unwrap();
        notify(
            vm,
            encoder,
            ProposeRunCompletionAckMessage { completion_id: 1 },
        );
        assert_eq!(vm.state(), AttemptState::Running);
        assert_eq!(take_notifications(vm), vec![]);
        end_with(vm, "my-output");
    });

    assert_end(&mut output, "my-output");
}

#[test]
fn end_fails_the_traces_left_open() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        vm.trace_start("model".to_owned(), None).unwrap();
        end_with(vm, "my-output");
    });

    output.next_decoded::<RunCommandMessage>().unwrap();
    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Failure(Failure {
                code: 500,
                message: "the handler ended before the trace".to_owned(),
                metadata: vec![],
            })),
        }
    );
    assert_end(&mut output, "my-output");
}

// --- Heartbeat

#[test]
fn heartbeat_says_what_the_attempt_waits_for() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        vm.heartbeat().unwrap();
        call(vm);
        vm.heartbeat().unwrap();
        end_with(vm, "my-output");
    });

    let heartbeat = |waiting_completions: Vec<u32>| AwaitingOnMessage {
        awaiting_on: Some(Future {
            waiting_completions,
            waiting_signals: vec![CANCEL_SIGNAL_ID],
            combinator_type: CombinatorType::FirstCompleted as i32,
            ..Default::default()
        }),
        executing_side_effects: true,
    };
    assert_eq!(
        {
            let mut heartbeat = output.next_decoded::<AwaitingOnMessage>().unwrap();
            sort_completions(&mut heartbeat.awaiting_on);
            heartbeat
        },
        heartbeat(vec![])
    );
    output.next_decoded::<CallCommandMessage>().unwrap();
    // Only the result of the call, not its invocation id
    assert_eq!(
        {
            let mut heartbeat = output.next_decoded::<AwaitingOnMessage>().unwrap();
            sort_completions(&mut heartbeat.awaiting_on);
            heartbeat
        },
        // Every pending completion: the invocation id too
        heartbeat(vec![1, 2])
    );
    assert_end(&mut output, "my-output");
}

#[test]
fn trace_names_its_parent() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        let parent = vm.trace_start("turn".to_owned(), None).unwrap();
        vm.trace_start("model".to_owned(), Some(parent)).unwrap();
        assert_that!(
            vm.trace_start("tool".to_owned(), Some(TraceId::from(42))),
            err(has_error_code(codes::INTERNAL))
        );
    });

    assert_eq!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        RunCommandMessage {
            result_completion_id: 1,
            name: "turn".to_owned(),
        }
    );
    assert_eq!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        RunCommandMessage {
            result_completion_id: 2,
            name: "model (parent trace 1)".to_owned(),
        }
    );
    output.next_decoded::<ErrorMessage>().unwrap();
    assert_eq!(output.next(), None);
}

// --- Clear all

#[test]
fn clear_all_state_makes_every_key_known() {
    let mut output = running(start_with_state(false, &[("a", "1")])).run(|vm, encoder| {
        vm.get_input().unwrap();
        let ack_handle = vm.cmd_state_clear_all(None, true).unwrap();
        set(vm, "b", "2");
        let a = get(vm, "a");
        let b = get(vm, "b");
        let keys = vm.cmd_state_get_keys(None).unwrap();
        notify(vm, encoder, CommandAckMessage { command_index: 1 });
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(a, Value::Void),
                notification(b, success("2")),
                notification(keys, Value::StateKeys(vec!["b".to_owned()])),
                notification(ack_handle, Value::Void),
            ]
        );
        end_with(vm, "my-output");
    });

    let (header, _) = output
        .next_with_header_decoded::<ClearAllStateCommandMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    output.next_decoded::<SetStateCommandMessage>().unwrap();
    assert_end(&mut output, "my-output");
}

#[test]
fn clear_all_state_answers_unknown_keys_locally() {
    let mut output = running(start_message(1)).run(|vm, _| {
        vm.get_input().unwrap();
        let cleared = vm.cmd_state_clear_all(None, false).unwrap();
        notified_at_once(vm, cleared);
        let read = get(vm, "never-sent");
        assert_eq!(
            take_notifications(vm),
            vec![notification(read, Value::Void)]
        );
        end_with(vm, "my-output");
    });

    output
        .next_decoded::<ClearAllStateCommandMessage>()
        .unwrap();
    assert_end(&mut output, "my-output");
}
