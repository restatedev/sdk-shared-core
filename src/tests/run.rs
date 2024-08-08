use super::*;

use crate::service_protocol::messages::{
    run_entry_message, EndMessage, EntryAckMessage, ErrorMessage, InputEntryMessage,
    OutputEntryMessage, RunEntryMessage, StartMessage, SuspensionMessage,
};
use assert2::let_assert;
use test_log::test;

#[test]
fn run_guard() {
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
                vm.sys_state_get("Personaggio".to_owned()),
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
fn exit_without_enter() {
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

            assert_that!(
                vm.sys_run_exit(NonEmptyValue::Success(vec![1, 2, 3])),
                err(eq_vm_error(vm::errors::INVOKED_RUN_EXIT_WITHOUT_ENTER))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(vm::errors::INVOKED_RUN_EXIT_WITHOUT_ENTER)
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
        .input(start_message(2))
        .input(input_entry_message(b"my-data"))
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

#[test]
fn enter_then_notify_error() {
    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();
            let_assert!(
                RunEnterResult::NotExecuted =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            vm.notify_error(
                Cow::Borrowed("my-error"),
                Cow::Borrowed("my-error-description"),
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_vm_error(VMError {
            code: 500,
            message: Cow::Borrowed("my-error"),
            description: Cow::Borrowed("my-error-description"),
        })
    );
    assert_eq!(output.next(), None);
}

mod consecutive_run {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        // First run
        let_assert!(RunEnterResult::NotExecuted = vm.sys_run_enter("".to_owned()).unwrap());
        let h1 = vm
            .sys_run_exit(NonEmptyValue::Success(b"Francesco".to_vec()))
            .unwrap();
        vm.notify_await_point(h1);
        let h1_result = vm.take_async_result(h1);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
            return;
        }
        let_assert!(Some(Value::Success(h1_value)) = h1_result.unwrap());

        // Second run
        let_assert!(RunEnterResult::NotExecuted = vm.sys_run_enter("".to_owned()).unwrap());
        let h2 = vm
            .sys_run_exit(NonEmptyValue::Success(Vec::from(
                String::from_utf8(h1_value).unwrap().to_uppercase(),
            )))
            .unwrap();
        vm.notify_await_point(h2);
        let h2_result = vm.take_async_result(h2);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h2_result {
            return;
        }
        let_assert!(Some(Value::Success(h2_value)) = h2_result.unwrap());

        // Write the result as output
        vm.sys_write_output(NonEmptyValue::Success(
            format!("Hello {}", String::from_utf8(h2_value).unwrap()).into_bytes(),
        ))
        .unwrap();
        vm.sys_end().unwrap();
    }

    #[test]
    fn without_acks_suspends() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage {
                value: Bytes::from_static(b"Till"),
                ..Default::default()
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            RunEntryMessage {
                result: Some(run_entry_message::Result::Value(Bytes::from_static(
                    b"Francesco"
                ))),
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
    fn ack_on_first_side_effect_will_suspend() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage {
                value: Bytes::from_static(b"Till"),
                ..Default::default()
            })
            .input(EntryAckMessage { entry_index: 1 })
            .run(handler);

        assert_eq!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            RunEntryMessage {
                result: Some(run_entry_message::Result::Value(Bytes::from_static(
                    b"Francesco"
                ))),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            RunEntryMessage {
                result: Some(run_entry_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO"
                ))),
                ..Default::default()
            }
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
    fn ack_on_first_and_second_side_effect_will_resume() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage {
                value: Bytes::from_static(b"Till"),
                ..Default::default()
            })
            .input(EntryAckMessage { entry_index: 1 })
            .input(EntryAckMessage { entry_index: 2 })
            .run(handler);

        assert_eq!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            RunEntryMessage {
                result: Some(run_entry_message::Result::Value(Bytes::from_static(
                    b"Francesco"
                ))),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            RunEntryMessage {
                result: Some(run_entry_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO"
                ))),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"Hello FRANCESCO"
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
}
