use super::*;

use crate::service_protocol::messages::send_signal_command_message::SignalId;
use crate::service_protocol::messages::*;
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::PayloadOptions;
use crate::Value;
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
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(
                    call_handle.invocation_id_notification_handle
                ))
                .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert2::assert!(
                let Some(Value::InvocationId(invocation_id)) = vm
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
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(
                    send_handle.invocation_id_notification_handle
                ))
                .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert2::assert!(
                let Some(Value::InvocationId(invocation_id)) = vm
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

#[test]
fn call_with_scope_and_limit_key_propagates_to_message() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            vm.sys_call(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    scope: Some("tenant-a".to_string()),
                    limit_key: Some("user-42".to_string()),
                    headers: Vec::new(),
                },
                Bytes::new(),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            scope: eq(Some("tenant-a".to_string())),
            limit_key: eq(Some("user-42".to_string())),
        })
    );
}

#[test]
fn send_with_scope_and_limit_key_propagates_to_message() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            vm.sys_send(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    scope: Some("tenant-a".to_string()),
                    limit_key: Some("user-42".to_string()),
                    headers: Vec::new(),
                },
                Bytes::new(),
                None,
                None,
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<OneWayCallCommandMessage>().unwrap(),
        pat!(OneWayCallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            scope: eq(Some("tenant-a".to_string())),
            limit_key: eq(Some("user-42".to_string())),
        })
    );
}

#[test]
fn call_with_empty_scope_errors() {
    let mut vm = CoreVM::mock_init(Version::maximum_supported_version());
    let encoder = Encoder::new(Version::maximum_supported_version());
    vm.notify_input(encoder.encode(&start_message(1)));
    vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
    vm.notify_input_closed();
    vm.sys_input().unwrap();

    let err = vm
        .sys_call(
            Target {
                service: "MySvc".to_string(),
                handler: "MyHandler".to_string(),
                key: None,
                idempotency_key: None,
                scope: Some(String::new()),
                limit_key: None,
                headers: Vec::new(),
            },
            Bytes::new(),
            None,
            PayloadOptions::default(),
        )
        .unwrap_err();

    assert_that!(err, eq_error(crate::vm::errors::EMPTY_SCOPE));
}

#[test]
fn call_with_empty_limit_key_errors() {
    let mut vm = CoreVM::mock_init(Version::maximum_supported_version());
    let encoder = Encoder::new(Version::maximum_supported_version());
    vm.notify_input(encoder.encode(&start_message(1)));
    vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
    vm.notify_input_closed();
    vm.sys_input().unwrap();

    let err = vm
        .sys_call(
            Target {
                service: "MySvc".to_string(),
                handler: "MyHandler".to_string(),
                key: None,
                idempotency_key: None,
                scope: Some("tenant-a".to_string()),
                limit_key: Some(String::new()),
                headers: Vec::new(),
            },
            Bytes::new(),
            None,
            PayloadOptions::default(),
        )
        .unwrap_err();

    assert_that!(err, eq_error(crate::vm::errors::EMPTY_LIMIT_KEY));
}

#[test]
fn call_with_scope_on_v6_returns_unsupported_feature() {
    let mut vm = CoreVM::mock_init(Version::V6);
    let encoder = Encoder::new(Version::V6);
    vm.notify_input(encoder.encode(&start_message(1)));
    vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
    vm.notify_input_closed();
    vm.sys_input().unwrap();

    let err = vm
        .sys_call(
            Target {
                service: "MySvc".to_string(),
                handler: "MyHandler".to_string(),
                key: None,
                idempotency_key: None,
                scope: Some("tenant-a".to_string()),
                limit_key: None,
                headers: Vec::new(),
            },
            Bytes::new(),
            None,
            PayloadOptions::default(),
        )
        .unwrap_err();

    assert_eq!(
        err.code,
        crate::vm::errors::codes::UNSUPPORTED_FEATURE.code()
    );
}

#[test]
fn call_with_limit_key_on_v6_returns_unsupported_feature() {
    let mut vm = CoreVM::mock_init(Version::V6);
    let encoder = Encoder::new(Version::V6);
    vm.notify_input(encoder.encode(&start_message(1)));
    vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
    vm.notify_input_closed();
    vm.sys_input().unwrap();

    let err = vm
        .sys_call(
            Target {
                service: "MySvc".to_string(),
                handler: "MyHandler".to_string(),
                key: None,
                idempotency_key: None,
                scope: None,
                limit_key: Some("user-42".to_string()),
                headers: Vec::new(),
            },
            Bytes::new(),
            None,
            PayloadOptions::default(),
        )
        .unwrap_err();

    assert_eq!(
        err.code,
        crate::vm::errors::codes::UNSUPPORTED_FEATURE.code()
    );
}
