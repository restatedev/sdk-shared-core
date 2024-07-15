use super::*;

use crate::service_protocol::messages::{
    run_entry_message, EndMessage, EntryAckMessage, ErrorMessage, InputEntryMessage,
    OutputEntryMessage, RunEntryMessage, StartMessage, SuspensionMessage,
};
use assert2::let_assert;
use test_log::test;

#[test]
fn side_effect_guard() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let_assert!(RunEnterResult::NotExecuted = vm.sys_run_enter("".to_owned()).unwrap());
            assert_that!(
                vm.sys_get_state("Personaggio".to_owned()),
                err(eq_vm_error(vm::errors::INSIDE_RUN))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(vm::errors::INSIDE_RUN)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_exit_then_suspend() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            let handle = vm
                .sys_run_exit(NonEmptyValue::Success(b"123".to_vec()))
                .unwrap();
            vm.notify_await_point(handle);

            // Not yet closed, we could still receive the ack here
            assert_that!(vm.take_async_result(handle), ok(none()));

            // Input closed, we won't receive the ack anymore
            vm.notify_input_closed();
            assert_that!(vm.take_async_result(handle), err(is_suspended()));
        });

    assert_that!(
        output.next_decoded::<RunEntryMessage>().unwrap(),
        eq(RunEntryMessage {
            name: "my-side-effect".to_owned(),
            result: Some(run_entry_message::Result::Value(Bytes::from_static(b"123"))),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_with_index(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_exit_then_ack() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            let handle = vm
                .sys_run_exit(NonEmptyValue::Success(b"123".to_vec()))
                .unwrap();
            vm.notify_await_point(handle);

            // Send the ack and close the input
            vm.notify_input(encoder.encode(&EntryAckMessage { entry_index: 1 }).into());
            vm.notify_input_closed();

            // We should now get the side effect result
            let result = vm.take_async_result(handle).unwrap().unwrap();
            let_assert!(Value::Success(s) = result);

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Success(s)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<RunEntryMessage>().unwrap(),
        eq(RunEntryMessage {
            name: "my-side-effect".to_owned(),
            result: Some(run_entry_message::Result::Value(Bytes::from_static(b"123"))),
        })
    );
    assert_that!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        has_output_success(b"123")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn replay() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .input(RunEntryMessage {
            name: "my-side-effect".to_owned(),
            result: Some(run_entry_message::Result::Value(Bytes::from_static(b"123"))),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::Executed(NonEmptyValue::Success(s)) =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Success(s)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        has_output_success(b"123")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}
