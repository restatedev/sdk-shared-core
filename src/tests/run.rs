use super::*;

use crate::service_protocol::messages::{
    propose_run_completion_message, run_completion_notification_message, EndMessage, ErrorMessage,
    Failure, OutputCommandMessage, ProposeRunCompletionMessage, RunCommandMessage,
    RunCompletionNotificationMessage, SleepCommandMessage, SleepCompletionNotificationMessage,
    StartMessage, SuspensionMessage,
};
use crate::PayloadOptions;
use assert2::let_assert;
use test_log::test;

#[test]
fn enter_then_propose_completion_then_suspend() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();

            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ExecuteRun(handle)
            );

            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            // Not yet closed, we could still receive the completion here
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ReadFromInput
            );

            // Input closed, we won't receive the ack anymore
            vm.notify_input_closed();
            assert_that!(vm.do_progress(vec![handle]), err(is_suspended()));
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
    );
    assert_that!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        eq(ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"123")
            )),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_propose_completion_then_complete() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ExecuteRun(handle)
            );

            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            vm.notify_input(encoder.encode(&RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Value(
                    Bytes::from_static(b"123").into(),
                )),
            }));
            vm.notify_input_closed();

            // We should now get the side effect result
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::AnyCompleted
            );
            let result = vm.take_notification(handle).unwrap().unwrap();
            let_assert!(Value::Success(s) = result);

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Success(s), PayloadOptions::default())
                .unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            name: "my-side-effect".to_owned(),
            result_completion_id: 1
        })
    );
    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"123")
            )),
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"123")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_propose_completion_then_complete_with_failure() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();

            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ExecuteRun(handle)
            );
            vm.propose_run_completion(
                handle,
                RunExitResult::TerminalFailure(TerminalFailure {
                    code: 500,
                    message: "my-failure".to_string(),
                    metadata: vec![],
                }),
                RetryPolicy::default(),
            )
            .unwrap();

            vm.notify_input(encoder.encode(&RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Failure(
                    Failure {
                        code: 500,
                        message: "my-failure".to_string(),
                        metadata: vec![],
                    },
                )),
            }));
            vm.notify_input_closed();

            // We should now get the side effect result
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::AnyCompleted
            );
            let result = vm.take_notification(handle).unwrap().unwrap();
            let_assert!(Value::Failure(f) = result);

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Failure(f), PayloadOptions::default())
                .unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
    );
    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Failure(Failure {
                code: 500,
                message: "my-failure".to_string(),
                metadata: vec![],
            })),
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_failure(500, "my-failure")
    );
    output.next_decoded::<EndMessage>().unwrap();
    assert_eq!(output.next(), None);
}

#[test]
fn enter_then_notify_input_closed_then_propose_completion() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            partial_state: false,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ExecuteRun(handle)
            );

            // Notify input closed here
            vm.notify_input_closed();

            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::WaitingPendingRun
            );

            // Propose run completion
            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(vm.do_progress(vec![handle]), err(is_suspended()));
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            name: "my-side-effect".to_owned(),
            result_completion_id: 1
        })
    );
    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"123")
            )),
        }
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn replay_without_completion() {
    let mut output = VMTestCase::new()
        .input(start_message(2))
        .input(input_entry_message(b"my-data"))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();

            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::ExecuteRun(handle)
            );
            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(vm.do_progress(vec![handle]), err(is_suspended()));
        });

    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"123")
            )),
        }
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn replay_without_completion_with_any() {
    let mut output = VMTestCase::new()
        .input(start_message(5))
        .input(input_entry_message(b"my-data"))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
        .input(SleepCommandMessage {
            wake_up_time: 0,
            result_completion_id: 2,
            ..Default::default()
        })
        .input(SleepCommandMessage {
            wake_up_time: 0,
            result_completion_id: 3,
            ..Default::default()
        })
        .input(SleepCompletionNotificationMessage {
            completion_id: 2,
            void: Some(Default::default()),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let run_handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
            let first_sleep_handle = vm
                .sys_sleep(Default::default(), Duration::ZERO, None)
                .unwrap();

            // await any(run, first_sleep), we're still replaying here!
            assert_eq!(
                vm.do_progress(vec![run_handle, first_sleep_handle])
                    .unwrap(),
                DoProgressResponse::AnyCompleted
            );
            assert!(vm.is_replaying());

            // Now we try to run!
            let second_sleep_handle = vm
                .sys_sleep(Default::default(), Duration::ZERO, None)
                .unwrap();
            assert!(vm.is_processing());
            assert_that!(
                vm.do_progress(vec![run_handle, second_sleep_handle]),
                ok(eq(DoProgressResponse::ExecuteRun(run_handle)))
            );

            vm.propose_run_completion(
                run_handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(
                vm.do_progress(vec![run_handle, second_sleep_handle]),
                err(is_suspended())
            );
        });

    assert_eq!(
        output
            .next_decoded::<ProposeRunCompletionMessage>()
            .unwrap(),
        ProposeRunCompletionMessage {
            result_completion_id: 1,
            result: Some(propose_run_completion_message::Result::Value(
                Bytes::from_static(b"123")
            )),
        }
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            waiting_completions: unordered_elements_are![eq(1), eq(3)],
            waiting_signals: eq(vec![1])
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn replay_with_completion() {
    let mut output = VMTestCase::new()
        .input(start_message(3))
        .input(input_entry_message(b"my-data"))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
        .input(RunCompletionNotificationMessage {
            completion_id: 1,
            result: Some(run_completion_notification_message::Result::Value(
                Bytes::from_static(b"123").into(),
            )),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert_eq!(
                vm.do_progress(vec![handle]).unwrap(),
                DoProgressResponse::AnyCompleted
            );

            // We should now get the side effect result
            let result = vm.take_notification(handle).unwrap().unwrap();
            let_assert!(Value::Success(s) = result);

            // Write the result as output
            vm.sys_write_output(NonEmptyValue::Success(s), PayloadOptions::default())
                .unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
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

            let _ = vm.sys_run("my-side-effect".to_owned()).unwrap();

            vm.notify_error(
                Error::internal(Cow::Borrowed("my-error"))
                    .with_stacktrace(Cow::Borrowed("my-error-description")),
                None,
            );
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 1,
            name: "my-side-effect".to_owned(),
        })
    );
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(
            Error::new(500u16, "my-error").with_stacktrace("my-error-description")
        )
    );
    assert_eq!(output.next(), None);
}

mod retry_policy {
    use super::*;
    use rstest::rstest;

    use crate::service_protocol::messages::SleepCommandMessage;
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
                ..start_message(2)
            })
            .input(input_entry_message(b"my-data"))
            .input(RunCommandMessage {
                result_completion_id: 1,
                name: "my-side-effect".to_string(),
            })
            .input(RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Failure(
                    Failure {
                        code: 500,
                        message: "my-error".to_string(),
                        metadata: vec![],
                    },
                )),
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
                vm.propose_run_completion(
                    handle,
                    RunExitResult::RetryableFailure {
                        error: Error::internal("my-error").with_stacktrace("my-stacktrace"),
                        attempt_duration,
                    },
                    retry_policy,
                )
                .unwrap();

                assert_eq!(
                    vm.do_progress(vec![handle]).unwrap(),
                    DoProgressResponse::AnyCompleted
                );
                let value = vm.take_notification(handle).unwrap().unwrap();

                // Write the result as output
                vm.sys_write_output(
                    match value {
                        Value::Success(s) => NonEmptyValue::Success(s),
                        Value::Failure(f) => NonEmptyValue::Failure(f),
                        v => panic!("Unexpected value {v:?}"),
                    },
                    PayloadOptions::default(),
                )
                .unwrap();
                vm.sys_end().unwrap();
            });

        assert_eq!(
            output
                .next_decoded::<ProposeRunCompletionMessage>()
                .unwrap(),
            ProposeRunCompletionMessage {
                result_completion_id: 1,
                result: Some(propose_run_completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "my-error".to_string(),
                    metadata: vec![],
                },)),
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
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
                let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(vm
                    .propose_run_completion(
                        handle,
                        RunExitResult::RetryableFailure {
                            error: Error::internal("my-error").with_stacktrace("my-stacktrace"),
                            attempt_duration,
                        },
                        retry_policy,
                    )
                    .is_err());
            });

        assert_that!(
            output.next_decoded::<RunCommandMessage>().unwrap(),
            eq(RunCommandMessage {
                result_completion_id: 1,
                name: "my-side-effect".to_owned(),
            })
        );
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(500),
                message: eq("my-error".to_string()),
                next_retry_delay: eq(next_retry_interval.map(|d| d.as_millis() as u64)),
                stacktrace: eq("my-stacktrace".to_string()),
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

    #[rstest]
    #[case(0, 0)]
    #[case(0, 1)]
    #[case(1, 2)]
    #[case(2, 2)]
    #[case(2, 1)]
    #[case(2, 0)]
    #[case(99, 100)]
    #[test_log::test]
    fn should_stop_retrying_with_retry_count_and_max_attempts(
        #[case] retry_count_since_last_stored_entry: usize,
        #[case] max_attempts: usize,
    ) {
        test_should_stop_retrying(
            retry_count_since_last_stored_entry as u32,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: Some(max_attempts as u32),
                max_duration: None,
                max_interval: None,
            },
        );
    }

    #[rstest]
    #[case(0, 2)]
    #[case(1, 3)]
    #[case(99, 101)]
    #[test_log::test]
    fn should_continue_retrying_with_retry_count_and_max_attempts(
        #[case] retry_count_since_last_stored_entry: usize,
        #[case] max_attempts: usize,
    ) {
        test_should_continue_retrying(
            retry_count_since_last_stored_entry as u32,
            Duration::from_secs(0),
            Duration::from_secs(0),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: Some(max_attempts as u32),
                max_duration: None,
                max_interval: None,
            },
            Some(Duration::from_secs(1)),
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_duration() {
        test_should_stop_retrying(
            0,
            Duration::from_secs(0),
            Duration::from_secs(1),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: None,
                max_duration: Some(Duration::from_secs(1)),
                max_interval: None,
            },
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
                interval: Some(Duration::from_secs(1)),
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
                interval: Some(Duration::from_secs(1)),
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
                interval: Some(Duration::from_secs(1)),
                max_attempts: Some(10),
                max_duration: None,
            },
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_exhausted_max_attempts_0() {
        test_should_stop_retrying(
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: Some(0),
                max_duration: None,
                max_interval: None,
            },
        );
    }

    #[test]
    fn exit_with_retryable_error_retry_policy_exhausted_max_attempts_1() {
        test_should_stop_retrying(
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: Some(0),
                max_duration: None,
                max_interval: None,
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
                vm.sys_sleep(String::default(), Duration::from_secs(100), None)
                    .unwrap();

                // Now try to enter run
                let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();

                assert!(vm
                    .propose_run_completion(
                        handle,
                        RunExitResult::RetryableFailure {
                            error: Error::internal("my-error"),
                            attempt_duration: Duration::from_millis(99)
                        },
                        RetryPolicy::FixedDelay {
                            interval: Some(Duration::from_secs(1)),
                            max_attempts: Some(2),
                            max_duration: Some(Duration::from_millis(100)),
                        }
                    )
                    .is_err());
            });

        let _ = output.next_decoded::<SleepCommandMessage>().unwrap();
        assert_that!(
            output.next_decoded::<RunCommandMessage>().unwrap(),
            eq(RunCommandMessage {
                result_completion_id: 2,
                name: "my-side-effect".to_owned(),
            })
        );
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
