use super::*;

use crate::service_protocol::messages::{
    ErrorMessage, GetLazyStateCommandMessage, OneWayCallCommandMessage, StartMessage,
};
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
        err(eq_vm_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES))
    );

    let mut output = OutputIterator::collect_vm(&mut vm);
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES)
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

fn test_entry_mismatch_on_replay<M: RestateMessage + Clone, T: fmt::Debug>(
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

            assert_that!(
                user_code(vm),
                err(eq_vm_error(
                    vm::errors::CommandMismatchError::new(1, expected.clone(), actual.clone())
                        .into()
                ))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(
            vm::errors::CommandMismatchError::new(1, expected, actual).into()
        )
    );
    assert_eq!(output.next(), None);
}
