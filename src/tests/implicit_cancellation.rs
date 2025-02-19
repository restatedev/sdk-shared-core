use super::*;

use crate::service_protocol::messages::*;
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::Value;
use assert2::let_assert;
use googletest::prelude::*;
use test_log::test;

#[test]
fn call_then_get_invocation_id_then_cancel() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                )
                .unwrap();

            // invocation id is here, let's take it and assert it
            assert_eq!(
                vm.do_progress(vec![call_handle.invocation_id_notification_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            let_assert!(
                Some(Value::InvocationId(invocation_id)) = vm
                    .take_notification(call_handle.invocation_id_notification_handle)
                    .unwrap()
            );
            assert_eq!(invocation_id, "my-id");

            // First time, responds with any completed, then it actually cancels
            assert_eq!(
                vm.do_progress(vec![call_handle.call_notification_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            assert_eq!(
                vm.do_progress(vec![call_handle.call_notification_handle])
                    .unwrap(),
                DoProgressResponse::CancelSignalReceived
            );

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn call_then_cancel() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                )
                .unwrap();

            // First time, responds with any completed, then it actually cancels
            assert_eq!(
                vm.do_progress(vec![call_handle.call_notification_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            assert_eq!(
                vm.do_progress(vec![call_handle.call_notification_handle])
                    .unwrap(),
                DoProgressResponse::CancelSignalReceived
            );

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn call_then_then_cancel_without_invocation_id() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                )
                .unwrap();

            // First time, responds with any completed, then suspends because it's missing the invocation id to complete the cancellation
            assert_eq!(
                vm.do_progress(vec![call_handle.call_notification_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            assert_that!(
                vm.do_progress(vec![call_handle.call_notification_handle]),
                err(is_suspended())
            );
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            waiting_completions: eq(vec![1]),
            waiting_signals: empty()
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn call_then_then_cancel_disabling_children_cancellation() {
    let mut output = VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Enabled {
            cancel_children_calls: false,
            cancel_children_one_way_calls: true,
        },
    })
    .input(start_message(1))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .run(|vm| {
        vm.sys_input().unwrap();

        let call_handle = vm
            .sys_call(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    headers: Vec::new(),
                },
                Bytes::new(),
            )
            .unwrap();

        // First time, responds with any completed, then suspends because it's missing the invocation id to complete the cancellation
        assert_eq!(
            vm.do_progress(vec![call_handle.call_notification_handle])
                .unwrap(),
            DoProgressResponse::AnyCompleted
        );
        assert_eq!(
            vm.do_progress(vec![call_handle.call_notification_handle])
                .unwrap(),
            DoProgressResponse::CancelSignalReceived
        );

        vm.sys_end().unwrap();
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn disabled_implicit_cancellation() {
    let mut output = VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Disabled,
    })
    .input(start_message(1))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .run(|vm| {
        vm.sys_input().unwrap();

        let call_handle = vm
            .sys_call(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    headers: Vec::new(),
                },
                Bytes::new(),
            )
            .unwrap();

        // Just suspended
        assert_that!(
            vm.do_progress(vec![call_handle.call_notification_handle]),
            err(is_suspended())
        );
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
            result_completion_id: eq(2),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            waiting_completions: eq(vec![2]),
            waiting_signals: empty()
        })
    );
    assert_eq!(output.next(), None);
}
