use super::*;

use crate::service_protocol::messages::start_message::StateEntry;
use crate::service_protocol::messages::{
    completion_message, get_state_entry_message, output_entry_message, CompletionMessage,
    EndMessage, GetStateEntryMessage, InputEntryMessage, OutputEntryMessage, StartMessage,
    SuspensionMessage,
};
use assert2::let_assert;
use test_log::test;

#[test]
fn just_replay() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .input(GetStateEntryMessage {
            key: Bytes::from_static(b"Personaggio"),
            result: Some(get_state_entry_message::Result::Value(Bytes::from_static(
                b"Pippo",
            ))),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_state_get("Personaggio".to_owned()).unwrap();
            let_assert!(Some(Value::Success(b)) = vm.take_async_result(handle).unwrap());
            assert_eq!(b, b"Pippo".to_vec());

            vm.sys_write_output(NonEmptyValue::Success(b)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Pippo"
            ))),
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
fn suspend() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_state_get("Personaggio".to_owned()).unwrap();

            // The callback should be completed immediately
            vm.notify_await_point(handle);
            assert_that!(
                vm.take_async_result(handle),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
        });

    assert_eq!(
        output.next_decoded::<GetStateEntryMessage>().unwrap(),
        GetStateEntryMessage {
            key: Bytes::from_static(b"Personaggio"),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        SuspensionMessage {
            entry_indexes: vec![1],
        }
    );
    assert_eq!(output.next(), None);
}

#[test]
fn completed() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CompletionMessage {
            entry_index: 1,
            result: Some(completion_message::Result::Value(Bytes::from_static(
                b"Pippo",
            ))),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_state_get("Personaggio".to_owned()).unwrap();

            vm.notify_await_point(handle);
            let_assert!(Some(Value::Success(b)) = vm.take_async_result(handle).unwrap());
            assert_eq!(b, b"Pippo".to_vec());

            vm.sys_write_output(NonEmptyValue::Success(b)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<GetStateEntryMessage>().unwrap(),
        GetStateEntryMessage {
            key: Bytes::from_static(b"Personaggio"),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Pippo"
            ))),
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
fn completed_with_eager_state() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            state_map: vec![StateEntry {
                key: Bytes::from_static(b"Personaggio"),
                value: Bytes::from_static(b"Francesco"),
            }],
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_state_get("Personaggio".to_owned()).unwrap();

            vm.notify_await_point(handle);
            let_assert!(Some(Value::Success(b)) = vm.take_async_result(handle).unwrap());
            assert_eq!(b, b"Francesco".to_vec());

            vm.sys_write_output(NonEmptyValue::Success(b)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<GetStateEntryMessage>().unwrap(),
        GetStateEntryMessage {
            key: Bytes::from_static(b"Personaggio"),
            result: Some(get_state_entry_message::Result::Value(Bytes::from_static(
                b"Francesco"
            ))),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Francesco"
            ))),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
