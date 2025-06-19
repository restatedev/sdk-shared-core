use super::*;

use crate::service_protocol::messages::{
    propose_run_completion_message, run_completion_notification_message, EndMessage, ErrorMessage,
    Failure, OutputCommandMessage, ProposeRunCompletionMessage, RunCommandMessage,
    RunCompletionNotificationMessage, StartMessage, SuspensionMessage,
};
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
            vm.sys_write_output(NonEmptyValue::Success(s)).unwrap();
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
            vm.sys_write_output(NonEmptyValue::Failure(f)).unwrap();
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
            vm.sys_write_output(NonEmptyValue::Success(s)).unwrap();
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
        error_message_as_vm_error(
            Error::new(500u16, "my-error").with_stacktrace("my-error-description")
        )
    );
    assert_eq!(output.next(), None);
}

mod retry_policy {
    use super::*;

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
                    },
                )),
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                let handle = vm.sys_run("my-side-effect".to_owned()).unwrap();
                vm.propose_run_completion(
                    handle,
                    RunExitResult::RetryableFailure {
                        error: Error::internal("my-error"),
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
                vm.sys_write_output(match value {
                    Value::Success(s) => NonEmptyValue::Success(s),
                    Value::Failure(f) => NonEmptyValue::Failure(f),
                    v => panic!("Unexpected value {v:?}"),
                })
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
                            error: Error::internal("my-error"),
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
                            interval: Duration::from_secs(1),
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
