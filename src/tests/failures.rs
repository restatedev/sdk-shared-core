use super::*;

use crate::service_protocol::messages::{
    ErrorMessage, GetStateEntryMessage, InputEntryMessage, OneWayCallEntryMessage, StartMessage,
};
use std::fmt;
use test_log::test;

#[test]
fn got_closed_stream_before_end_of_replay() {
    let mut vm = CoreVM::mock_init(Version::V1);
    let encoder = Encoder::new(Version::V1);

    vm.notify_input(encoder.encode(&StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        // 2 expected entries!
        known_entries: 2,
        ..Default::default()
    }));
    vm.notify_input(encoder.encode(&InputEntryMessage::default()));

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
fn get_state_entry_mismatch() {
    test_entry_mismatch(
        GetStateEntryMessage {
            key: Bytes::from_static(b"my-key"),
            ..Default::default()
        },
        GetStateEntryMessage {
            key: Bytes::from_static(b"another-key"),
            ..Default::default()
        },
        |vm| vm.sys_state_get("another-key".to_owned()),
    );
}

#[test]
fn one_way_call_entry_mismatch() {
    test_entry_mismatch(
        OneWayCallEntryMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"123"),
            ..Default::default()
        },
        OneWayCallEntryMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"456"),
            ..Default::default()
        },
        |vm| {
            vm.sys_send(
                Target {
                    service: "greeter".to_owned(),
                    handler: "greet".to_owned(),
                    key: Some("my-key".to_owned()),
                },
                Bytes::from_static(b"456"),
                None,
            )
        },
    );
}

fn test_entry_mismatch<M: WriteableRestateMessage + Clone, T: fmt::Debug>(
    expected: M,
    actual: M,
    user_code: impl FnOnce(&mut CoreVM) -> Result<T, VMError>,
) {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            partial_state: true,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .input(expected.clone())
        .run(|vm| {
            vm.sys_input().unwrap();

            assert_that!(
                user_code(vm),
                err(eq_vm_error(
                    vm::errors::EntryMismatchError::new(expected.clone(), actual.clone(),).into()
                ))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(vm::errors::EntryMismatchError::new(expected, actual,).into())
    );
    assert_eq!(output.next(), None);
}
