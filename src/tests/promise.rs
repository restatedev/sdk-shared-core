use super::*;

use crate::service_protocol::messages::{Failure, *};

mod get_promise {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_get_promise("my-prom".to_owned()).unwrap();
        vm.notify_await_point(h1);
        let h1_result = vm.take_async_result(h1);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
            return;
        }

        let output = match h1_result.unwrap().expect("Should be ready") {
            Value::Void => {
                panic!("Got void result, unexpected for get promise")
            }
            Value::Success(s) => NonEmptyValue::Success(s),
            Value::Failure(f) => NonEmptyValue::Failure(f),
            v => panic!("Unexpected value {v:?}"),
        };

        vm.sys_write_output(output).unwrap();
        vm.sys_end().unwrap();
    }

    #[test]
    fn completed_with_success() {
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
                    b"\"my value\"",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<GetPromiseEntryMessage>().unwrap(),
            GetPromiseEntryMessage {
                key: "my-prom".to_owned(),
                ..Default::default()
            }
        );

        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"\"my value\""
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
    fn completed_with_failure() {
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
                result: Some(completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "myerror".to_owned(),
                })),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<GetPromiseEntryMessage>().unwrap(),
            GetPromiseEntryMessage {
                key: "my-prom".to_owned(),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Failure(Failure {
                    code: 500,
                    message: "myerror".to_owned(),
                })),
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

mod peek_promise {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_peek_promise("my-prom".to_owned()).unwrap();
        vm.notify_await_point(h1);
        let h1_result = vm.take_async_result(h1);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
            return;
        }

        let output = match h1_result.unwrap().expect("Should be ready") {
            Value::Void => NonEmptyValue::Success("null".into()),
            Value::Success(s) => NonEmptyValue::Success(s),
            Value::Failure(f) => NonEmptyValue::Failure(f),
            v => panic!("Unexpected value {v:?}"),
        };

        vm.sys_write_output(output).unwrap();
        vm.sys_end().unwrap();
    }

    #[test]
    fn completed_with_success() {
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
                    b"\"my value\"",
                ))),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseEntryMessage>().unwrap(),
            PeekPromiseEntryMessage {
                key: "my-prom".to_owned(),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"\"my value\""
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
    fn completed_with_failure() {
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
                result: Some(completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "myerror".to_owned(),
                })),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseEntryMessage>().unwrap(),
            PeekPromiseEntryMessage {
                key: "my-prom".to_owned(),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Failure(Failure {
                    code: 500,
                    message: "myerror".to_owned(),
                })),
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
    fn completed_with_null() {
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
                result: Some(completion_message::Result::Empty(Empty::default())),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseEntryMessage>().unwrap(),
            PeekPromiseEntryMessage {
                key: "my-prom".to_owned(),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(Bytes::from_static(
                    b"null"
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

mod complete_promise {
    use super::*;

    use test_log::test;

    const RESOLVED: Bytes = Bytes::from_static(b"true");
    const REJECTED: Bytes = Bytes::from_static(b"false");

    fn handler(result: NonEmptyValue) -> impl FnOnce(&mut CoreVM) {
        |vm| {
            vm.sys_input().unwrap();

            let h1 = vm
                .sys_complete_promise("my-prom".to_owned(), result)
                .unwrap();
            vm.notify_await_point(h1);
            let h1_result = vm.take_async_result(h1);
            if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
                return;
            }

            let output = match h1_result.unwrap().expect("Should be ready") {
                Value::Void => RESOLVED,
                Value::Success(_) => panic!("Unexpected success completion"),
                Value::Failure(_) => REJECTED,
                v => panic!("Unexpected value {v:?}"),
            };

            vm.sys_write_output(NonEmptyValue::Success(output.to_vec()))
                .unwrap();
            vm.sys_end().unwrap();
        }
    }

    #[test]
    fn resolve_promise_succeeds() {
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
                result: Some(completion_message::Result::Empty(Empty::default())),
            })
            .run(handler(NonEmptyValue::Success(b"my val".to_vec())));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseEntryMessage>()
                .unwrap(),
            CompletePromiseEntryMessage {
                key: "my-prom".to_owned(),
                completion: Some(complete_promise_entry_message::Completion::CompletionValue(
                    Bytes::from_static(b"my val")
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(RESOLVED)),
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
    fn resolve_promise_fails() {
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
                result: Some(completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "cannot write promise".to_owned(),
                })),
            })
            .run(handler(NonEmptyValue::Success(b"my val".to_vec())));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseEntryMessage>()
                .unwrap(),
            CompletePromiseEntryMessage {
                key: "my-prom".to_owned(),
                completion: Some(complete_promise_entry_message::Completion::CompletionValue(
                    Bytes::from_static(b"my val")
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(REJECTED)),
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
    fn reject_promise_succeeds() {
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
                result: Some(completion_message::Result::Empty(Empty::default())),
            })
            .run(handler(NonEmptyValue::Failure(
                Failure {
                    code: 500,
                    message: "my failure".to_owned(),
                }
                .into(),
            )));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseEntryMessage>()
                .unwrap(),
            CompletePromiseEntryMessage {
                key: "my-prom".to_owned(),
                completion: Some(
                    complete_promise_entry_message::Completion::CompletionFailure(Failure {
                        code: 500,
                        message: "my failure".to_owned(),
                    })
                ),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(RESOLVED)),
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
    fn reject_promise_fails() {
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
                result: Some(completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "cannot write promise".to_owned(),
                })),
            })
            .run(handler(NonEmptyValue::Failure(
                Failure {
                    code: 500,
                    message: "my failure".to_owned(),
                }
                .into(),
            )));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseEntryMessage>()
                .unwrap(),
            CompletePromiseEntryMessage {
                key: "my-prom".to_owned(),
                completion: Some(
                    complete_promise_entry_message::Completion::CompletionFailure(Failure {
                        code: 500,
                        message: "my failure".to_owned(),
                    })
                ),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            OutputEntryMessage {
                result: Some(output_entry_message::Result::Value(REJECTED)),
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
