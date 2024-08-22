use super::*;

use crate::service_protocol::messages::{
    run_entry_message, EndMessage, EntryAckMessage, ErrorMessage, InputEntryMessage,
    OutputEntryMessage, RunEntryMessage, StartMessage, SuspensionMessage,
};
use assert2::let_assert;
use test_log::test;

#[test]
fn run_guard() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted { .. } = vm.sys_run_enter("".to_owned()).unwrap()
            );
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
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            assert_that!(
                vm.sys_run_exit(
                    RunExitResult::Success(vec![1, 2, 3].into()),
                    RetryPolicy::default()
                ),
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
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted { .. } =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            let handle = vm
                .sys_run_exit(
                    RunExitResult::Success(Bytes::from_static(b"123")),
                    RetryPolicy::default(),
                )
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
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted { .. } =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            let handle = vm
                .sys_run_exit(
                    RunExitResult::Success(Bytes::from_static(b"123")),
                    RetryPolicy::default(),
                )
                .unwrap();
            vm.notify_await_point(handle);

            // Send the ack and close the input
            vm.notify_input(encoder.encode(&EntryAckMessage { entry_index: 1 }));
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
        is_output_with_success(b"123")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_exit_then_ack_with_failure() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let_assert!(
                RunEnterResult::NotExecuted { .. } =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            let handle = vm
                .sys_run_exit(
                    RunExitResult::TerminalFailure(Failure {
                        code: 500,
                        message: "my-failure".to_string(),
                    }),
                    RetryPolicy::default(),
                )
                .unwrap();
            vm.notify_await_point(handle);

            // Send the ack and close the input
            vm.notify_input(encoder.encode(&EntryAckMessage { entry_index: 1 }));
            vm.notify_input_closed();

            // We should now get the side effect result
            let result = vm.take_async_result(handle).unwrap().unwrap();
            let_assert!(Value::Failure(f) = result);

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Failure(f)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<RunEntryMessage>().unwrap(),
        eq(RunEntryMessage {
            name: "my-side-effect".to_owned(),
            result: Some(run_entry_message::Result::Failure(messages::Failure {
                code: 500,
                message: "my-failure".to_string(),
            })),
        })
    );
    assert_that!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        is_output_with_failure(500, "my-failure")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn replay() {
    let mut output = VMTestCase::new()
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
        is_output_with_success(b"123")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_notify_error() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();
            let_assert!(
                RunEnterResult::NotExecuted { .. } =
                    vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
            );
            vm.notify_error(
                Cow::Borrowed("my-error"),
                Cow::Borrowed("my-error-description"),
                None,
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
        let_assert!(RunEnterResult::NotExecuted { .. } = vm.sys_run_enter("".to_owned()).unwrap());
        let h1 = vm
            .sys_run_exit(
                RunExitResult::Success(Bytes::from_static(b"Francesco")),
                RetryPolicy::default(),
            )
            .unwrap();
        vm.notify_await_point(h1);
        let h1_result = vm.take_async_result(h1);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
            return;
        }
        let_assert!(Some(Value::Success(h1_value)) = h1_result.unwrap());

        // Second run
        let_assert!(RunEnterResult::NotExecuted { .. } = vm.sys_run_enter("".to_owned()).unwrap());
        let h2 = vm
            .sys_run_exit(
                RunExitResult::Success(Bytes::from(
                    String::from_utf8_lossy(&h1_value).to_uppercase(),
                )),
                RetryPolicy::default(),
            )
            .unwrap();
        vm.notify_await_point(h2);
        let h2_result = vm.take_async_result(h2);
        if let Err(SuspendedOrVMError::Suspended(_)) = &h2_result {
            return;
        }
        let_assert!(Some(Value::Success(h2_value)) = h2_result.unwrap());

        // Write the result as output
        vm.sys_write_output(NonEmptyValue::Success(
            format!("Hello {}", String::from_utf8(h2_value.to_vec()).unwrap())
                .into_bytes()
                .into(),
        ))
        .unwrap();
        vm.sys_end().unwrap();
    }

    #[test]
    fn without_acks_suspends() {
        let mut output = VMTestCase::new()
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
        let mut output = VMTestCase::new()
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
        let mut output = VMTestCase::new()
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
        assert_that!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            is_output_with_success(b"Hello FRANCESCO"),
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }
}

mod retry_policy {
    use super::*;

    use crate::service_protocol::messages::AwakeableEntryMessage;
    use test_log::test;

    fn test_should_stop_retrying(
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
        attempt_duration: Duration,
        retry_policy: RetryPolicy,
    ) {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                retry_count_since_last_stored_entry,
                duration_since_last_stored_entry: duration_since_last_stored_entry.as_millis()
                    as u64,
                ..start_message(1)
            })
            .input(input_entry_message(b"my-data"))
            .input(EntryAckMessage { entry_index: 1 })
            .run(|vm| {
                vm.sys_input().unwrap();
                let_assert!(
                    RunEnterResult::NotExecuted { .. } =
                        vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
                );
                let handle = vm
                    .sys_run_exit(
                        RunExitResult::RetryableFailure {
                            failure: Failure {
                                code: 500,
                                message: "my-error".to_string(),
                            },
                            attempt_duration,
                        },
                        retry_policy,
                    )
                    .unwrap();

                vm.notify_await_point(handle);
                let handle_result = vm.take_async_result(handle);
                if let Err(SuspendedOrVMError::Suspended(_)) = &handle_result {
                    return;
                }
                let_assert!(Some(value) = handle_result.unwrap());

                // Write the result as output
                vm.sys_write_output(match value {
                    Value::Success(s) => NonEmptyValue::Success(s),
                    Value::Failure(f) => NonEmptyValue::Failure(f),
                    v => panic!("Unexpected value {v:?}"),
                })
                .unwrap();
                vm.sys_end().unwrap();
            });

        assert_that!(
            output.next_decoded::<RunEntryMessage>().unwrap(),
            is_run_with_failure(500, "my-error")
        );
        assert_that!(
            output.next_decoded::<OutputEntryMessage>().unwrap(),
            is_output_with_failure(500, "my-error"),
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    fn test_should_continue_retrying(
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
        attempt_duration: Duration,
        retry_policy: RetryPolicy,
        next_retry_interval: Option<Duration>,
    ) {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                retry_count_since_last_stored_entry,
                duration_since_last_stored_entry: duration_since_last_stored_entry.as_millis()
                    as u64,
                ..start_message(1)
            })
            .input(input_entry_message(b"my-data"))
            .run(|vm| {
                vm.sys_input().unwrap();
                let_assert!(
                    RunEnterResult::NotExecuted { .. } =
                        vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
                );
                assert!(vm
                    .sys_run_exit(
                        RunExitResult::RetryableFailure {
                            failure: Failure {
                                code: 500,
                                message: "my-error".to_string(),
                            },
                            attempt_duration
                        },
                        retry_policy
                    )
                    .is_err());
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(500),
                message: eq("my-error".to_string()),
                next_retry_delay: eq(next_retry_interval.map(|d| d.as_millis() as u64))
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn exit_with_retryable_error_no_retry_policy() {
        test_should_continue_retrying(
            0,
            Duration::ZERO,
            Duration::ZERO,
            RetryPolicy::Infinite,
            None,
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_none() {
        test_should_stop_retrying(0, Duration::ZERO, Duration::ZERO, RetryPolicy::None)
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_fixed() {
        test_should_continue_retrying(
            0,
            Duration::ZERO,
            Duration::ZERO,
            RetryPolicy::FixedDelay {
                interval: Duration::from_secs(1),
                max_attempts: None,
                max_duration: None,
            },
            Some(Duration::from_secs(1)),
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_exhausted_max_duration() {
        test_should_stop_retrying(
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::FixedDelay {
                interval: Duration::from_secs(1),
                max_attempts: None,
                max_duration: Some(Duration::from_secs(2)),
            },
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_exhausted_max_attempts() {
        test_should_stop_retrying(
            9,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::FixedDelay {
                interval: Duration::from_secs(1),
                max_attempts: Some(10),
                max_duration: None,
            },
        );
    }

    #[test]
    fn retry_info_is_zero_when_entry_is_the_one_after_the_first_new_entry() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                retry_count_since_last_stored_entry: 10,
                duration_since_last_stored_entry: Duration::from_secs(10).as_millis() as u64,
                ..start_message(1)
            })
            .input(input_entry_message(b"my-data"))
            .run(|vm| {
                vm.sys_input().unwrap();

                // Just create another journal entry
                vm.sys_awakeable().unwrap();

                // Now try to enter run
                let_assert!(
                    RunEnterResult::NotExecuted(retry_info) =
                        vm.sys_run_enter("my-side-effect".to_owned()).unwrap()
                );

                // This is not the first processed entry of this attempt,
                // so the info in StartMessage should are invalid now
                assert_eq!(retry_info.retry_count, 0);
                assert_eq!(retry_info.retry_loop_duration, Duration::ZERO);

                assert!(vm
                    .sys_run_exit(
                        RunExitResult::RetryableFailure {
                            failure: Failure {
                                code: 500,
                                message: "my-error".to_string(),
                            },
                            attempt_duration: Duration::from_millis(99)
                        },
                        RetryPolicy::FixedDelay {
                            interval: Duration::from_secs(1),
                            max_attempts: Some(2),
                            max_duration: Some(Duration::from_millis(100)),
                        }
                    )
                    .is_err());
            });

        let _ = output.next_decoded::<AwakeableEntryMessage>().unwrap();
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(500),
                message: eq("my-error".to_string()),
                next_retry_delay: some(eq(Duration::from_secs(1).as_millis() as u64))
            })
        );
        assert_eq!(output.next(), None);
    }
}
