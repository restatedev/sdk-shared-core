use super::*;

use crate::error::CommandMetadata;
use crate::service_protocol::messages::start_message::StateEntry;
use crate::service_protocol::messages::{
    CallCommandMessage, CommandMessageHeaderDiff, EndMessage, ErrorMessage,
    GetEagerStateCommandMessage, GetLazyStateCommandMessage, OneWayCallCommandMessage,
    SetStateCommandMessage, StartMessage,
};
use crate::service_protocol::MessageType;
use std::fmt;
use std::result::Result;
use test_log::test;

#[test]
fn got_closed_stream_before_end_of_replay() {
    let mut vm = CoreVM::mock_init(Version::maximum_supported_version());
    let encoder = Encoder::new(Version::maximum_supported_version());

    vm.notify_input(encoder.encode(&StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        // 2 expected entries!
        known_entries: 2,
        ..Default::default()
    }));
    vm.notify_input(encoder.encode(&InputCommandMessage::default()));

    // Now notify input closed
    vm.notify_input_closed();

    // Try to check if input is ready, this should fail
    assert_that!(
        vm.is_ready_to_execute(),
        err(eq_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES))
    );

    let mut output = OutputIterator::collect_vm(&mut vm);
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn explicit_error_notification() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            vm.notify_error(
                Error::internal(Cow::Borrowed("my-error"))
                    .with_next_retry_delay_override(Duration::from_secs(10)),
                Some(CommandRelationship::Next {
                    ty: CommandType::Call,
                    name: None,
                }),
            );
        });
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(500u32),
            message: eq("my-error".to_owned()),
            related_command_type: eq(Some(u16::from(MessageType::CallCommand) as u32)),
            related_command_index: eq(Some(1)),
            next_retry_delay: eq(Some(Duration::from_secs(10).as_millis() as u64)),
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn get_lazy_state_entry_mismatch() {
    test_entry_mismatch_on_replay(
        GetLazyStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            result_completion_id: 1,
            ..Default::default()
        },
        GetLazyStateCommandMessage {
            key: Bytes::from_static(b"another-key"),
            result_completion_id: 1,
            ..Default::default()
        },
        |vm| vm.sys_state_get("another-key".to_owned()),
    );
}

#[test]
fn one_way_call_entry_mismatch() {
    test_entry_mismatch_on_replay(
        OneWayCallCommandMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"123"),
            invocation_id_notification_idx: 1,
            ..Default::default()
        },
        OneWayCallCommandMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"456"),
            invocation_id_notification_idx: 1,
            ..Default::default()
        },
        |vm| {
            vm.sys_send(
                Target {
                    service: "greeter".to_owned(),
                    handler: "greet".to_owned(),
                    key: Some("my-key".to_owned()),
                    idempotency_key: None,
                    headers: Vec::new(),
                },
                Bytes::from_static(b"456"),
                None,
            )
        },
    );
}

fn test_entry_mismatch_on_replay<
    M: RestateMessage + CommandMessageHeaderDiff + Clone,
    T: fmt::Debug,
>(
    expected: M,
    actual: M,
    user_code: impl FnOnce(&mut CoreVM) -> Result<T, Error>,
) {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            partial_state: true,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .input(expected.clone())
        .run(|vm| {
            vm.sys_input().unwrap();

            let expected_error = Error::from(vm::errors::CommandMismatchError::new(
                1,
                expected.clone(),
                actual.clone(),
            ))
            .with_related_command_metadata(CommandMetadata::new(1, M::ty()));
            assert_that!(user_code(vm), err(eq(expected_error)));
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(vm::errors::CommandMismatchError::new(1, expected, actual).into())
    );
    assert_eq!(output.next(), None);
}

#[test]
fn disable_non_deterministic_payload_checks() {
    let mut output = VMTestCase::with_vm_options(VMOptions {
        non_determinism_checks: NonDeterministicChecksOption::PayloadChecksDisabled,
        ..VMOptions::default()
    })
    .input(StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries: 5,
        partial_state: true,
        state_map: vec![StateEntry {
            key: Bytes::from_static(b"STATE"),
            // NOTE: this is different payload than the one recorded in the entry!
            value: Bytes::from_static(b"456"),
        }],
        ..Default::default()
    })
    .input(input_entry_message(b"my-data"))
    .input(OneWayCallCommandMessage {
        service_name: "greeter".to_owned(),
        handler_name: "greet".to_owned(),
        key: "my-key".to_owned(),
        parameter: Bytes::from_static(b"123"),
        invocation_id_notification_idx: 1,
        ..Default::default()
    })
    .input(CallCommandMessage {
        service_name: "greeter".to_owned(),
        handler_name: "greet".to_owned(),
        key: "my-key".to_owned(),
        parameter: Bytes::from_static(b"123"),
        invocation_id_notification_idx: 2,
        result_completion_id: 3,
        ..Default::default()
    })
    .input(SetStateCommandMessage {
        key: Bytes::from_static(b"my-key"),
        value: Some(messages::Value {
            content: Bytes::from_static(b"123"),
        }),
        ..Default::default()
    })
    .input(GetEagerStateCommandMessage {
        key: Bytes::from_static(b"STATE"),
        result: Some(messages::get_eager_state_command_message::Result::Value(
            messages::Value {
                content: Bytes::from_static(b"123"),
            },
        )),
        ..Default::default()
    })
    .run(|vm| {
        vm.sys_input().unwrap();

        vm.sys_send(
            Target {
                service: "greeter".to_owned(),
                handler: "greet".to_owned(),
                key: Some("my-key".to_owned()),
                idempotency_key: None,
                headers: Vec::new(),
            },
            // NOTE: this is different payload!
            Bytes::from_static(b"456"),
            None,
        )
        .unwrap();

        vm.sys_call(
            Target {
                service: "greeter".to_owned(),
                handler: "greet".to_owned(),
                key: Some("my-key".to_owned()),
                idempotency_key: None,
                headers: Vec::new(),
            },
            // NOTE: this is different payload!
            Bytes::from_static(b"456"),
        )
        .unwrap();

        vm.sys_state_set(
            "my-key".to_owned(),
            // NOTE: this is different payload!
            Bytes::from_static(b"456"),
        )
        .unwrap();

        vm.sys_state_get("STATE".to_owned()).unwrap();

        vm.sys_end().unwrap();
    });

    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
