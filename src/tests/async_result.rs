use super::*;

use crate::service_protocol::messages::*;
use assert2::let_assert;

use test_log::test;

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greeter".to_string(),
        key: None,
    }
}

#[test]
fn dont_await_call() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputEntryMessage::default())
        .run(|vm| {
            vm.sys_input().unwrap();

            let _ = vm
                .sys_call(greeter_target(), b"Francesco".to_vec())
                .unwrap();
            vm.sys_write_output(NonEmptyValue::Success(b"Whatever".to_vec()))
                .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallEntryMessage>().unwrap(),
        CallEntryMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Whatever"
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
fn dont_await_call_dont_notify_input_closed() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputEntryMessage::default())
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();
            let _ = vm
                .sys_call(greeter_target(), b"Francesco".to_vec())
                .unwrap();
            vm.sys_write_output(NonEmptyValue::Success(b"Whatever".to_vec()))
                .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallEntryMessage>().unwrap(),
        CallEntryMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Whatever"
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

mod notify_await_point {
    use super::*;

    use test_log::test;

    #[test]
    fn await_twice_the_same_handle() {
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

                let (_, h) = vm.sys_awakeable().unwrap();

                vm.notify_await_point(h);
                vm.notify_await_point(h);

                vm.notify_input_closed();
            });

        assert_eq!(
            output.next_decoded::<AwakeableEntryMessage>().unwrap(),
            AwakeableEntryMessage::default()
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
    fn await_two_handles_at_same_time() {
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

                let (_, h1) = vm.sys_awakeable().unwrap();
                let (_, h2) = vm.sys_awakeable().unwrap();

                vm.notify_await_point(h1);
                // This should transition the state machine to error
                vm.notify_await_point(h2);

                vm.notify_input_closed();
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
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_vm_error(
                vm::errors::AwaitingTwoAsyncResultError {
                    previous: 1,
                    current: 2,
                }
                .into()
            )
        );
        assert_eq!(output.next(), None);
    }
}

mod reverse_await_order {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm
            .sys_call(greeter_target(), b"Francesco".to_vec())
            .unwrap();
        let h2 = vm.sys_call(greeter_target(), b"Till".to_vec()).unwrap();

        vm.notify_await_point(h2);
        let h2_result = vm.take_async_result(h2);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h2_result {
            return;
        }
        let_assert!(Some(Value::Success(h2_value)) = h2_result.unwrap());

        vm.sys_state_set("A2".to_owned(), h2_value.clone()).unwrap();

        vm.notify_await_point(h1);
        let h1_result = vm.take_async_result(h1);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
            return;
        }
        let_assert!(Some(Value::Success(h1_value)) = h1_result.unwrap());

        vm.sys_write_output(NonEmptyValue::Success(
            [&h1_value[..], b"-", &h2_value[..]].concat(),
        ))
        .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn none_completed() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage::default())
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
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
    fn a1_and_a2_completed_later() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage::default())
            .input(CompletionMessage {
                entry_index: 1,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO",
                ))),
            })
            .input(CompletionMessage {
                entry_index: 2,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"TILL",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateEntryMessage>().unwrap(),
            SetStateEntryMessage {
                key: Bytes::from_static(b"A2"),
                value: Bytes::from_static(b"TILL"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO-TILL"
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
    fn a2_and_a1_completed_later() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage::default())
            .input(CompletionMessage {
                entry_index: 2,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"TILL",
                ))),
            })
            .input(CompletionMessage {
                entry_index: 1,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateEntryMessage>().unwrap(),
            SetStateEntryMessage {
                key: Bytes::from_static(b"A2"),
                value: Bytes::from_static(b"TILL"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO-TILL"
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
    fn only_a2_completed() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage::default())
            .input(CompletionMessage {
                entry_index: 2,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"TILL",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateEntryMessage>().unwrap(),
            SetStateEntryMessage {
                key: Bytes::from_static(b"A2"),
                value: Bytes::from_static(b"TILL"),
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
    fn only_a1_completed() {
        let mut output = VMTestCase::new(Version::V1)
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputEntryMessage::default())
            .input(CompletionMessage {
                entry_index: 1,
                result: Some(completion_message::Result::Value(Bytes::from_static(
                    b"FRANCESCO",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallEntryMessage>().unwrap(),
            CallEntryMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
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
}
