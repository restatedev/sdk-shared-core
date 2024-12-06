use super::*;

use crate::service_protocol::messages::*;
use assert2::let_assert;
use googletest::prelude::*;
use test_log::test;

#[test]
fn call_then_get_invocation_id_then_cancel_invocation() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CompletionMessage {
            entry_index: 2,
            result: Some(completion_message::Result::Value(Bytes::from_static(
                b"my-id",
            ))),
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
                )
                .unwrap();

            let invocation_id_handle = vm
                .sys_get_call_invocation_id(GetInvocationIdTarget::CallEntry(call_handle))
                .unwrap();
            vm.notify_await_point(invocation_id_handle);
            let_assert!(
                Some(Value::InvocationId(invocation_id)) =
                    vm.take_async_result(invocation_id_handle).unwrap()
            );
            assert_eq!(invocation_id, "my-id");

            vm.sys_cancel_invocation(CancelInvocationTarget::CallEntry(call_handle))
                .unwrap();
            vm.sys_cancel_invocation(CancelInvocationTarget::InvocationId(invocation_id.clone()))
                .unwrap();

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallEntryMessage>().unwrap(),
        pat!(CallEntryMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler")
        })
    );
    assert_eq!(
        output
            .next_decoded::<GetCallInvocationIdEntryMessage>()
            .unwrap(),
        GetCallInvocationIdEntryMessage {
            call_entry_index: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        output
            .next_decoded::<CancelInvocationEntryMessage>()
            .unwrap(),
        CancelInvocationEntryMessage {
            target: Some(cancel_invocation_entry_message::Target::CallEntryIndex(1)),
            ..Default::default()
        }
    );
    assert_eq!(
        output
            .next_decoded::<CancelInvocationEntryMessage>()
            .unwrap(),
        CancelInvocationEntryMessage {
            target: Some(cancel_invocation_entry_message::Target::InvocationId(
                "my-id".to_string()
            )),
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
        .input(CompletionMessage {
            entry_index: 2,
            result: Some(completion_message::Result::Value(Bytes::from_static(
                b"my-id",
            ))),
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
                )
                .unwrap();

            let invocation_id_handle = vm
                .sys_get_call_invocation_id(GetInvocationIdTarget::SendEntry(send_handle))
                .unwrap();
            vm.notify_await_point(invocation_id_handle);
            let_assert!(
                Some(Value::InvocationId(invocation_id)) =
                    vm.take_async_result(invocation_id_handle).unwrap()
            );
            assert_eq!(invocation_id, "my-id");

            vm.sys_cancel_invocation(CancelInvocationTarget::SendEntry(send_handle))
                .unwrap();
            vm.sys_cancel_invocation(CancelInvocationTarget::InvocationId(invocation_id.clone()))
                .unwrap();

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<OneWayCallEntryMessage>().unwrap(),
        pat!(OneWayCallEntryMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler")
        })
    );
    assert_eq!(
        output
            .next_decoded::<GetCallInvocationIdEntryMessage>()
            .unwrap(),
        GetCallInvocationIdEntryMessage {
            call_entry_index: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        output
            .next_decoded::<CancelInvocationEntryMessage>()
            .unwrap(),
        CancelInvocationEntryMessage {
            target: Some(cancel_invocation_entry_message::Target::CallEntryIndex(1)),
            ..Default::default()
        }
    );
    assert_eq!(
        output
            .next_decoded::<CancelInvocationEntryMessage>()
            .unwrap(),
        CancelInvocationEntryMessage {
            target: Some(cancel_invocation_entry_message::Target::InvocationId(
                "my-id".to_string()
            )),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
