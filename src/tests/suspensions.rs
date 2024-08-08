use super::*;

use crate::service_protocol::messages::{
    completion_message, AwakeableEntryMessage, CompletionMessage, EndMessage, GetStateEntryMessage,
    SuspensionMessage,
};
use test_log::test;

#[test]
fn suspension_should_be_triggered_in_notify_input_closed() {
    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            let _ = vm.sys_input().unwrap();

            let handle = vm.sys_state_get("Personaggio".to_owned()).unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_async_result(handle), ok(none()));

            // Let's notify_input_closed now
            vm.notify_input_closed();
            vm.notify_await_point(handle);
            assert_that!(
                vm.take_async_result(handle),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
        });

    // Assert output
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
fn suspension_should_be_triggered_with_correct_entry() {
    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let (_, _h1) = vm.sys_awakeable().unwrap();
            let (_, h2) = vm.sys_awakeable().unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_async_result(h2), ok(none()));

            // Let's notify_input_closed now
            vm.notify_await_point(h2);
            vm.notify_input_closed();
            assert_that!(
                vm.take_async_result(h2),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
        });

    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_eq!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        SuspensionMessage {
            entry_indexes: vec![2],
        }
    );
    assert_eq!(output.next(), None);
}

#[test]
fn when_notify_completion_then_notify_await_point_then_notify_input_closed_then_no_suspension() {
    let completion = Bytes::from_static(b"completion");

    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let (_, _h1) = vm.sys_awakeable().unwrap();
            let (_, h2) = vm.sys_awakeable().unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_async_result(h2), ok(none()));

            // Let's send Completion for h2
            vm.notify_input(
                encoder
                    .encode(&CompletionMessage {
                        entry_index: 2,
                        result: Some(completion_message::Result::Value(completion.clone())),
                    })
                    .to_vec(),
            );

            // Let's await for input h2, then notify_input_closed
            vm.notify_await_point(h2);
            vm.notify_input_closed();

            // This should not suspend
            assert_that!(
                vm.take_async_result(h2),
                ok(some(eq(Value::Success(completion.to_vec()))))
            );

            vm.sys_write_output(NonEmptyValue::Success(completion.to_vec()))
                .unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_that!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        has_output_success(completion)
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn when_notify_await_point_then_notify_completion_then_notify_input_closed_then_no_suspension() {
    let completion = Bytes::from_static(b"completion");

    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let (_, _h1) = vm.sys_awakeable().unwrap();
            let (_, h2) = vm.sys_awakeable().unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_async_result(h2), ok(none()));

            // Notify await point, then send completion, then close input
            vm.notify_await_point(h2);
            vm.notify_input(
                encoder
                    .encode(&CompletionMessage {
                        entry_index: 2,
                        result: Some(completion_message::Result::Value(completion.clone())),
                    })
                    .to_vec(),
            );
            vm.notify_input_closed();

            // This should not suspend
            assert_that!(
                vm.take_async_result(h2),
                ok(some(eq(Value::Success(completion.to_vec()))))
            );

            vm.sys_write_output(NonEmptyValue::Success(completion.to_vec()))
                .unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_eq!(
        output.next_decoded::<AwakeableEntryMessage>().unwrap(),
        AwakeableEntryMessage::default()
    );
    assert_that!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        has_output_success(completion)
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
