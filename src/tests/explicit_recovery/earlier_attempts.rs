//! What an attempt takes from the entries the earlier attempts stored.

use super::*;
use test_log::test;

use crate::service_protocol::messages::{
    call_completion_notification_message, propose_run_completion_message,
    run_completion_notification_message, CallCommandMessage, CallCompletionNotificationMessage,
    CallInvocationIdCompletionNotificationMessage, CommandAckMessage, Failure,
    ProposeRunCompletionAckMessage, ProposeRunCompletionMessage, RunCommandMessage,
    RunCompletionNotificationMessage,
};

/// The earlier attempts stored the input, a set state and a call (completion ids 1 and 2, and the
/// invocation id): 3 commands, 4 entries.
fn with_earlier_call() -> ErTestCase {
    ErTestCase::new()
        .input(start_message(4))
        .input(input_entry_message(b"my-input"))
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            value: Some(Bytes::from_static(b"my-value").into()),
            ..Default::default()
        })
        .input(CallCommandMessage {
            service_name: "MySvc".to_owned(),
            handler_name: "MyHandler".to_owned(),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..Default::default()
        })
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "earlier-call".to_owned(),
        })
}

fn call_result(completion_id: u32, value: &'static str) -> CallCompletionNotificationMessage {
    CallCompletionNotificationMessage {
        completion_id,
        result: Some(call_completion_notification_message::Result::Value(
            Bytes::from_static(value.as_bytes()).into(),
        )),
    }
}

#[test]
fn numbering_continues_after_the_earlier_commands() {
    let mut output = with_earlier_call().run(|vm, encoder| {
        vm.get_input().unwrap();

        // Notification ids from 1 + 2 * (3 - 1), command indexes from 3.
        let call = vm
            .cmd_call(target(), Bytes::new(), Default::default())
            .unwrap();
        let ack_handle = vm
            .cmd_state_set(
                "my-key".to_owned(),
                Bytes::from_static(b"my-new-value"),
                None,
                true,
            )
            .unwrap();

        // The call (index 3) asked for no ack
        notify(vm, encoder, CommandAckMessage { command_index: 3 });
        notify(vm, encoder, CommandAckMessage { command_index: 4 });
        notify(vm, encoder, call_result(6, "my-result"));
        assert_eq!(
            take_notifications(vm),
            vec![
                notification(ack_handle, Value::Void),
                notification(call.call_notification_handle, success("my-result"))
            ]
        );

        end_with(vm, "my-output");
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            invocation_id_notification_idx: eq(5),
            result_completion_id: eq(6)
        })
    );
    let (header, _) = output
        .next_with_header_decoded::<SetStateCommandMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    assert_end(&mut output, "my-output");
}

#[test]
fn responses_to_earlier_attempts_are_dropped() {
    let mut output = with_earlier_call().run(|vm, encoder| {
        vm.get_input().unwrap();

        // The result of the earlier call
        notify(vm, encoder, call_result(2, "earlier-result"));
        assert_eq!(take_notifications(vm), vec![]);

        // An id this attempt never used
        notify(vm, encoder, call_result(42, "unknown"));
        assert_eq!(take_notifications(vm), vec![]);
        assert_eq!(vm.state(), AttemptState::Running);

        end_with(vm, "my-output");
    });

    assert_end(&mut output, "my-output");
}

#[test]
fn notifications_before_the_handler_starts() {
    let mut output = ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        // Sent live, before the handler starts
        .input(call_result(1, "stale"))
        .run(|vm, _| {
            vm.get_input().unwrap();
            assert_eq!(take_notifications(vm), vec![]);
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn signals_other_than_the_cancellation_are_dropped() {
    let mut output = ErTestCase::new()
        .input(start_message(2))
        .input(input_entry_message(b"my-input"))
        .input(empty_signal_notification(17))
        .run(|vm, encoder| {
            vm.get_input().unwrap();
            notify(vm, encoder, empty_signal_notification(18));
            notify(vm, encoder, signal_notification("my-signal", b"my-value"));
            assert_eq!(take_notifications(vm), vec![]);
            assert!(!vm.is_cancelled());
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

// --- Cancellation

#[test]
fn not_cancelled_without_a_cancellation_signal() {
    let mut output = ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            assert!(!vm.is_cancelled());
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn cancellation_received_by_an_earlier_attempt() {
    let mut test_case = ErTestCase::new()
        .input(start_message(3))
        .input(input_entry_message(b"my-input"));
    // Not until the cancellation signal arrives
    assert!(!test_case.vm.is_cancelled());
    test_case = test_case.input(cancel_signal_notification());
    assert!(test_case.vm.is_cancelled());

    let mut output = test_case
        .input(SetStateCommandMessage::default())
        .run(|vm, _| {
            assert!(vm.is_cancelled());
            // A flag, not a notification
            assert_eq!(take_notifications(vm), vec![]);
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn cancellation_received_while_processing() {
    let mut output = ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, encoder| {
            vm.get_input().unwrap();
            assert!(!vm.is_cancelled());
            notify(vm, encoder, cancel_signal_notification());
            assert!(vm.is_cancelled());
            assert_eq!(take_notifications(vm), vec![]);

            // A second one changes nothing, and the attempt goes on
            notify(vm, encoder, cancel_signal_notification());
            assert!(vm.is_cancelled());
            assert_eq!(vm.state(), AttemptState::Running);
            end_with(vm, "my-output");

            // Still cancelled once the attempt is over
            assert!(vm.is_cancelled());
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn runs_left_open_by_earlier_attempts_are_failed() {
    let mut output = ErTestCase::new()
        .input(start_message(4))
        .input(input_entry_message(b"my-input"))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "model".to_owned(),
        })
        .input(RunCommandMessage {
            result_completion_id: 2,
            name: "tool".to_owned(),
        })
        .input(RunCompletionNotificationMessage {
            completion_id: 2,
            result: Some(run_completion_notification_message::Result::Value(
                Bytes::from_static(b"done").into(),
            )),
        })
        .run(|vm, encoder| {
            vm.get_input().unwrap();
            // The ack of the failure has an id below this attempt's completion ids
            notify(
                vm,
                encoder,
                ProposeRunCompletionAckMessage { completion_id: 1 },
            );
            assert_eq!(vm.state(), AttemptState::Running);
            end_with(vm, "my-output");
        });

    let (header, proposal) = output
        .next_with_header_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_eq!(header.requested_ack(), Some(true));
    assert_eq!(
        proposal,
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Failure(Failure {
                code: 500,
                message: "the attempt ended before the run completed".to_owned(),
                metadata: vec![],
            })),
        }
    );
    assert_end(&mut output, "my-output");
}
