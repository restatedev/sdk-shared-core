use super::*;

use crate::service_protocol::messages::*;
use assert2::let_assert;

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greeter".to_string(),
        key: None,
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

        vm.sys_set_state("A2".to_owned(), h2_value.clone()).unwrap();

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
