use super::*;

use crate::service_protocol::messages::{Failure, *};
use crate::Value;

mod get_promise {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_get_promise("my-prom".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let output = match vm.take_notification(h1).unwrap().expect("Should be ready") {
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
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(GetPromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(get_promise_completion_notification_message::Result::Value(
                    Bytes::from_static(b"\"my value\"").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<GetPromiseCommandMessage>().unwrap(),
            GetPromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"\"my value\"")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }
    #[test]
    fn completed_with_failure() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(GetPromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_promise_completion_notification_message::Result::Failure(Failure {
                        code: 500,
                        message: "myerror".to_owned(),
                    }),
                ),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<GetPromiseCommandMessage>().unwrap(),
            GetPromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_failure(500, "myerror")
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

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let output = match vm.take_notification(h1).unwrap().expect("Should be ready") {
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
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(PeekPromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(peek_promise_completion_notification_message::Result::Value(
                    Bytes::from_static(b"\"my value\"").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseCommandMessage>().unwrap(),
            PeekPromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"\"my value\"")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn completed_with_failure() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(PeekPromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    peek_promise_completion_notification_message::Result::Failure(Failure {
                        code: 500,
                        message: "myerror".to_owned(),
                    }),
                ),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseCommandMessage>().unwrap(),
            PeekPromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_failure(500, "myerror")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn completed_with_null() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(PeekPromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(peek_promise_completion_notification_message::Result::Void(
                    Default::default(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<PeekPromiseCommandMessage>().unwrap(),
            PeekPromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"null")
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

            if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
                assert_that!(
                    vm.take_notification(h1),
                    err(pat!(SuspendedOrVMError::Suspended(_)))
                );
                return;
            }
            let output = match vm.take_notification(h1).unwrap().expect("Should be ready") {
                Value::Void => RESOLVED,
                Value::Success(_) => panic!("Unexpected success completion"),
                Value::Failure(_) => REJECTED,
                v => panic!("Unexpected value {v:?}"),
            };

            vm.sys_write_output(NonEmptyValue::Success(output)).unwrap();
            vm.sys_end().unwrap();
        }
    }

    #[test]
    fn resolve_promise_succeeds() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CompletePromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    complete_promise_completion_notification_message::Result::Void(
                        Default::default(),
                    ),
                ),
            })
            .run(handler(NonEmptyValue::Success(Bytes::from_static(
                b"my val",
            ))));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseCommandMessage>()
                .unwrap(),
            CompletePromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                completion: Some(
                    complete_promise_command_message::Completion::CompletionValue(
                        Bytes::from_static(b"my val").into()
                    )
                ),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(RESOLVED)
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn resolve_promise_fails() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CompletePromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    complete_promise_completion_notification_message::Result::Failure(Failure {
                        code: 500,
                        message: "cannot write promise".to_owned(),
                    }),
                ),
            })
            .run(handler(NonEmptyValue::Success(Bytes::from_static(
                b"my val",
            ))));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseCommandMessage>()
                .unwrap(),
            CompletePromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                completion: Some(
                    complete_promise_command_message::Completion::CompletionValue(
                        Bytes::from_static(b"my val").into()
                    )
                ),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(REJECTED)
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn reject_promise_succeeds() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CompletePromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    complete_promise_completion_notification_message::Result::Void(
                        Default::default(),
                    ),
                ),
            })
            .run(handler(NonEmptyValue::Failure(TerminalFailure {
                code: 500,
                message: "my failure".to_owned(),
            })));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseCommandMessage>()
                .unwrap(),
            CompletePromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                completion: Some(
                    complete_promise_command_message::Completion::CompletionFailure(Failure {
                        code: 500,
                        message: "my failure".to_owned(),
                    })
                ),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(RESOLVED)
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn reject_promise_fails() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CompletePromiseCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    complete_promise_completion_notification_message::Result::Failure(Failure {
                        code: 500,
                        message: "cannot write promise".to_owned(),
                    }),
                ),
            })
            .run(handler(NonEmptyValue::Failure(TerminalFailure {
                code: 500,
                message: "my failure".to_owned(),
            })));

        assert_eq!(
            output
                .next_decoded::<CompletePromiseCommandMessage>()
                .unwrap(),
            CompletePromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                completion: Some(
                    complete_promise_command_message::Completion::CompletionFailure(Failure {
                        code: 500,
                        message: "my failure".to_owned(),
                    })
                ),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(REJECTED)
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }
}
