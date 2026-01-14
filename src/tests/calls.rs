use super::*;

use crate::service_protocol::messages::send_signal_command_message::SignalId;
use crate::service_protocol::messages::*;
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::PayloadOptions;
use crate::Value;
use assert2::let_assert;
use googletest::prelude::*;
use test_log::test;

#[test]
fn call_then_get_invocation_id_then_cancel_invocation() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
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
                    PayloadOptions::default(),
                )
                .unwrap();

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

            vm.sys_cancel_invocation(invocation_id).unwrap();

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
            signal_id: Some(SignalId::Idx(CANCEL_SIGNAL_ID)),
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
fn send_then_get_invocation_id_then_cancel_invocation() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let send_handle = vm
                .sys_send(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            assert_eq!(
                vm.do_progress(vec![send_handle.invocation_id_notification_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            let_assert!(
                Some(Value::InvocationId(invocation_id)) = vm
                    .take_notification(send_handle.invocation_id_notification_handle)
                    .unwrap()
            );
            assert_eq!(invocation_id, "my-id");

            vm.sys_cancel_invocation(invocation_id).unwrap();

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<OneWayCallCommandMessage>().unwrap(),
        pat!(OneWayCallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id".to_string(),
            signal_id: Some(SignalId::Idx(CANCEL_SIGNAL_ID)),
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
