use super::*;

use crate::error::CommandMetadata;
use crate::service_protocol::messages::start_message::StateEntry;
use crate::service_protocol::messages::{
    complete_awakeable_command_message, complete_promise_command_message, output_command_message,
    CallCommandMessage, CommandMessageHeaderDiff, CompleteAwakeableCommandMessage,
    CompletePromiseCommandMessage, EndMessage, ErrorMessage, GetEagerStateCommandMessage,
    GetLazyStateCommandMessage, OneWayCallCommandMessage, OutputCommandMessage,
    SetStateCommandMessage, StartMessage,
};
use crate::service_protocol::MessageType;
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
        err(eq_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES))
    );

    let mut output = OutputIterator::collect_vm(&mut vm);
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(vm::errors::INPUT_CLOSED_WHILE_WAITING_ENTRIES)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn explicit_error_notification() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            vm.notify_error(
                Error::internal(Cow::Borrowed("my-error"))
                    .with_next_retry_delay_override(Duration::from_secs(10)),
                Some(CommandRelationship::Next {
                    ty: CommandType::Call,
                    name: None,
                }),
            );
        });
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(500u32),
            message: eq("my-error".to_owned()),
            related_command_type: eq(Some(u16::from(MessageType::CallCommand) as u32)),
            related_command_index: eq(Some(1)),
            next_retry_delay: eq(Some(Duration::from_secs(10).as_millis() as u64)),
        })
    );
    assert_eq!(output.next(), None);
}

mod journal_mismatch {
    use super::*;
    use crate::error::NotificationMetadata;
    use crate::service_protocol::messages::{
        RunCommandMessage, SleepCommandMessage, SleepCompletionNotificationMessage,
    };
    use crate::service_protocol::{NotificationId, CANCEL_SIGNAL_ID};
    use crate::vm::awakeable_id_str;
    use std::collections::{HashMap, HashSet};
    use test_log::test;

    #[test]
    fn get_lazy_state_mismatch() {
        expect_mismatch_on_replay(
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
            |vm| vm.sys_state_get("another-key".to_owned(), PayloadOptions::default()),
        );
    }

    #[test]
    fn set_state_mismatch() {
        expect_mismatch_on_replay(
            SetStateCommandMessage {
                key: Bytes::from_static(b"my-key"),
                value: Some(messages::Value {
                    content: Bytes::from_static(b"my-value"),
                }),
                ..Default::default()
            },
            SetStateCommandMessage {
                key: Bytes::from_static(b"my-key"),
                value: Some(messages::Value {
                    content: Bytes::from_static(b"another-value"),
                }),
                ..Default::default()
            },
            |vm| {
                vm.sys_state_set(
                    "my-key".to_owned(),
                    Bytes::from_static(b"another-value"),
                    PayloadOptions::default(),
                )
            },
        );
    }

    #[test]
    fn one_way_call_mismatch() {
        expect_mismatch_on_replay(
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
                    PayloadOptions::default(),
                )
            },
        );
    }

    fn expect_mismatch_on_replay<
        M: RestateMessage + CommandMessageHeaderDiff + Clone,
        T: fmt::Debug,
    >(
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

                let expected_error = Error::from(vm::errors::CommandMismatchError::new(
                    1,
                    expected.clone(),
                    actual.clone(),
                ))
                .with_related_command_metadata(CommandMetadata::new(1, M::ty()));
                assert_that!(user_code(vm), err(eq(expected_error)));
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(
                vm::errors::CommandMismatchError::new(1, expected, actual).into()
            )
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn disable_non_deterministic_payload_checks_on_vm() {
        let mut output = VMTestCase::with_vm_options(VMOptions {
            non_determinism_checks: NonDeterministicChecksOption::PayloadChecksDisabled,
            ..VMOptions::default()
        })
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 5,
            partial_state: true,
            state_map: vec![StateEntry {
                key: Bytes::from_static(b"STATE"),
                // NOTE: this is different payload than the one recorded in the entry!
                value: Bytes::from_static(b"456"),
            }],
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .input(OneWayCallCommandMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"123"),
            invocation_id_notification_idx: 1,
            ..Default::default()
        })
        .input(CallCommandMessage {
            service_name: "greeter".to_owned(),
            handler_name: "greet".to_owned(),
            key: "my-key".to_owned(),
            parameter: Bytes::from_static(b"123"),
            invocation_id_notification_idx: 2,
            result_completion_id: 3,
            ..Default::default()
        })
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"my-key"),
            value: Some(messages::Value {
                content: Bytes::from_static(b"123"),
            }),
            ..Default::default()
        })
        .input(GetEagerStateCommandMessage {
            key: Bytes::from_static(b"STATE"),
            result: Some(messages::get_eager_state_command_message::Result::Value(
                messages::Value {
                    content: Bytes::from_static(b"123"),
                },
            )),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            vm.sys_send(
                Target {
                    service: "greeter".to_owned(),
                    handler: "greet".to_owned(),
                    key: Some("my-key".to_owned()),
                    idempotency_key: None,
                    headers: Vec::new(),
                },
                // NOTE: this is different payload!
                Bytes::from_static(b"456"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_call(
                Target {
                    service: "greeter".to_owned(),
                    handler: "greet".to_owned(),
                    key: Some("my-key".to_owned()),
                    idempotency_key: None,
                    headers: Vec::new(),
                },
                // NOTE: this is different payload!
                Bytes::from_static(b"456"),
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_state_set(
                "my-key".to_owned(),
                // NOTE: this is different payload!
                Bytes::from_static(b"456"),
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_state_get("STATE".to_owned(), PayloadOptions::default())
                .unwrap();

            vm.sys_end().unwrap();
        });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    // Tests for PayloadOptions::unstable() on different operations

    #[test]
    fn one_way_call_with_unstable_payload() {
        // When SDK marks the current call with unstable serialization,
        // payload equality check is skipped (we trust the SDK).
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(OneWayCallCommandMessage {
                service_name: "greeter".to_owned(),
                handler_name: "greet".to_owned(),
                key: "my-key".to_owned(),
                parameter: Bytes::from_static(b"123"),
                invocation_id_notification_idx: 1,
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_send(
                    Target {
                        service: "greeter".to_owned(),
                        handler: "greet".to_owned(),
                        key: Some("my-key".to_owned()),
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    // Different bytes than recorded, but current side is flagged.
                    Bytes::from_static(b"456"),
                    None,
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn call_with_unstable_payload() {
        // sys_call with PayloadOptions::unstable() should skip payload check
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(CallCommandMessage {
                service_name: "greeter".to_owned(),
                handler_name: "greet".to_owned(),
                key: "my-key".to_owned(),
                parameter: Bytes::from_static(b"123"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_call(
                    Target {
                        service: "greeter".to_owned(),
                        handler: "greet".to_owned(),
                        key: Some("my-key".to_owned()),
                        idempotency_key: None,
                        headers: Vec::new(),
                    },
                    // Different bytes than recorded, but marked as unstable.
                    Bytes::from_static(b"456"),
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn set_state_with_unstable_payload() {
        // sys_state_set with PayloadOptions::unstable() should skip payload check
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(SetStateCommandMessage {
                key: Bytes::from_static(b"my-key"),
                value: Some(messages::Value {
                    content: Bytes::from_static(b"123"),
                }),
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_state_set(
                    "my-key".to_owned(),
                    // Different bytes than recorded, but marked as unstable.
                    Bytes::from_static(b"456"),
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_eager_state_with_unstable_payload() {
        // sys_state_get with PayloadOptions::unstable() should skip payload check
        // when replaying GetEagerStateCommand with different value
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 2,
                partial_state: false,
                state_map: vec![StateEntry {
                    key: Bytes::from_static(b"my-key"),
                    // This is the "current" value, different from recorded
                    value: Bytes::from_static(b"456"),
                }],
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .input(GetEagerStateCommandMessage {
                key: Bytes::from_static(b"my-key"),
                result: Some(messages::get_eager_state_command_message::Result::Value(
                    messages::Value {
                        content: Bytes::from_static(b"123"),
                    },
                )),
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                // With unstable serialization, different payload should be accepted
                vm.sys_state_get("my-key".to_owned(), PayloadOptions::unstable())
                    .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn complete_promise_with_unstable_payload() {
        // sys_complete_promise with PayloadOptions::unstable() should skip payload check
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(CompletePromiseCommandMessage {
                key: "my-prom".to_owned(),
                result_completion_id: 1,
                completion: Some(
                    complete_promise_command_message::Completion::CompletionValue(
                        Bytes::from_static(b"123").into(),
                    ),
                ),
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_complete_promise(
                    "my-prom".to_owned(),
                    // Different bytes than recorded, but marked as unstable.
                    NonEmptyValue::Success(Bytes::from_static(b"456")),
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn complete_awakeable_with_unstable_payload() {
        // sys_complete_awakeable with PayloadOptions::unstable() should skip payload check
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(CompleteAwakeableCommandMessage {
                awakeable_id: "awk-123".to_owned(),
                result: Some(complete_awakeable_command_message::Result::Value(
                    Bytes::from_static(b"123").into(),
                )),
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_complete_awakeable(
                    "awk-123".to_owned(),
                    // Different bytes than recorded, but marked as unstable.
                    NonEmptyValue::Success(Bytes::from_static(b"456")),
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn write_output_with_unstable_payload() {
        // sys_write_output with PayloadOptions::unstable() should skip payload check
        let mut output = VMTestCase::new()
            .input(start_message(2))
            .input(input_entry_message(b"my-data"))
            .input(OutputCommandMessage {
                result: Some(output_command_message::Result::Value(
                    Bytes::from_static(b"123").into(),
                )),
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                vm.sys_write_output(
                    // Different bytes than recorded, but marked as unstable.
                    NonEmptyValue::Success(Bytes::from_static(b"456")),
                    PayloadOptions::unstable(),
                )
                .unwrap();

                vm.sys_end().unwrap();
            });

        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn add_await_run_after_progress_was_made() {
        let expected_error = Error::from(vm::errors::UncompletedDoProgressDuringReplay::new(
            HashSet::from([
                NotificationId::CompletionId(1),
                NotificationId::SignalId(CANCEL_SIGNAL_ID),
            ]),
            HashMap::from([
                (
                    NotificationId::CompletionId(1),
                    NotificationMetadata::RelatedToCommand(CommandMetadata::new_named(
                        "my-side-effect".to_owned(),
                        1,
                        MessageType::RunCommand,
                    )),
                ),
                (
                    NotificationId::SignalId(CANCEL_SIGNAL_ID),
                    NotificationMetadata::Cancellation,
                ),
            ]),
        ))
        .with_related_command_metadata(CommandMetadata::new_named(
            "my-side-effect".to_owned(),
            1,
            MessageType::RunCommand,
        ));

        let mut output = VMTestCase::new()
            .input(start_message(4))
            .input(input_entry_message(b"my-data"))
            // We have a run
            .input(RunCommandMessage {
                result_completion_id: 1,
                name: "my-side-effect".to_owned(),
            })
            // Then we have a sleep that is already completed
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 2,
                ..Default::default()
            })
            .input(SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(Default::default()),
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                let run_handle = vm.sys_run("my-side-effect".to_owned()).unwrap();

                // On await, this is the expected error
                assert_that!(
                    vm.do_progress(vec![run_handle]),
                    err(eq(expected_error.clone()))
                );
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(expected_error)
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn add_await_sleep_after_progress_was_made() {
        let expected_error = Error::from(vm::errors::UncompletedDoProgressDuringReplay::new(
            HashSet::from([
                NotificationId::CompletionId(1),
                NotificationId::SignalId(CANCEL_SIGNAL_ID),
            ]),
            HashMap::from([(
                NotificationId::SignalId(CANCEL_SIGNAL_ID),
                NotificationMetadata::Cancellation,
            )]),
        ));

        let mut output = VMTestCase::new()
            .input(start_message(4))
            .input(input_entry_message(b"my-data"))
            // We have the first sleep
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 1,
                ..Default::default()
            })
            // Then we have a sleep that is already completed
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 2,
                ..Default::default()
            })
            .input(SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(Default::default()),
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                // Simulating the user code to be:
                //
                // await sleep()
                // await sleep()
                //
                // But this journal could have been created only with the following code:
                // sleep()
                // await sleep()
                //
                // Otherwise the notification for the first sleep should be in the journal before the second sleep!

                let sleep_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();

                // On await, this is the expected error
                assert_that!(
                    vm.do_progress(vec![sleep_handle]),
                    err(eq(expected_error.clone()))
                );
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(expected_error)
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn add_await_awakeable_after_progress_was_made() {
        let invocation_id = Bytes::from_static(b"123");

        let expected_error = Error::from(vm::errors::UncompletedDoProgressDuringReplay::new(
            HashSet::from([
                NotificationId::SignalId(17),
                NotificationId::SignalId(CANCEL_SIGNAL_ID),
            ]),
            HashMap::from([
                (
                    NotificationId::SignalId(17),
                    NotificationMetadata::Awakeable(awakeable_id_str(&invocation_id, 17)),
                ),
                (
                    NotificationId::SignalId(CANCEL_SIGNAL_ID),
                    NotificationMetadata::Cancellation,
                ),
            ]),
        ));

        let mut output = VMTestCase::new()
            .input(messages::StartMessage {
                id: invocation_id,
                ..start_message(3)
            })
            .input(input_entry_message(b"my-data"))
            // Then we have a sleep that is already completed
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 2,
                ..Default::default()
            })
            .input(SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(Default::default()),
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                // Simulating the user code to be:
                //
                // await awakeable()
                // await sleep()
                //
                // But this journal could have been created only with the following code:
                // awakeable()
                // await sleep()
                //
                // Otherwise the notification for the awakeable should be in the journal before the second awakeable!

                let (_, awakeable_handle) = vm.sys_awakeable().unwrap();

                // On await, this is the expected error
                assert_that!(
                    vm.do_progress(vec![awakeable_handle]),
                    err(eq(expected_error.clone()))
                );
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(expected_error)
        );
        assert_eq!(output.next(), None);
    }
}
