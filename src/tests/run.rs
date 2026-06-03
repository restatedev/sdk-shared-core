use super::*;

use crate::service_protocol::messages::{
    propose_run_completion_message, run_completion_notification_message, AwaitingOnMessage,
    EndMessage, ErrorMessage, Failure, OutputCommandMessage, ProposeRunCompletionAckMessage,
    ProposeRunCompletionMessage, RunCommandMessage, RunCompletionNotificationMessage,
    SleepCommandMessage, SleepCompletionNotificationMessage, StartMessage, SuspensionMessage,
};
use crate::PayloadOptions;
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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::ExecuteRun(handle)
            );
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: true
                }
            );

            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            // Not yet closed, we could still receive the completion here
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            // Input closed, we won't receive the ack anymore
            vm.notify_input_closed();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(handle)),
                err(is_suspended())
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
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: eq(vec![1]),
                waiting_signals: eq(vec![1]),
                waiting_named_signals: empty(),
                nested_futures: empty(),
                combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
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
    let mut output = VMTestCase::with_version(Version::V6)
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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::ExecuteRun(handle)
            );

            // This should not generate AwaitingOnMessage
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: true
                }
            );

            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            // This will generate AwaitingOnMessage
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            vm.notify_input(encoder.encode(&RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Value(
                    Bytes::from_static(b"123").into(),
                )),
            }));
            vm.notify_input_closed();

            // We should now get the side effect result
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            let result = vm.take_notification(handle).unwrap().unwrap();
            assert2::assert!(let Value::Success(s) = result);

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
    let mut output = VMTestCase::with_version(Version::V6)
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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::ExecuteRun(handle)
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
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            let result = vm.take_notification(handle).unwrap().unwrap();
            assert2::assert!(let Value::Failure(f) = result);

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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::ExecuteRun(handle)
            );

            // Notify input closed here
            vm.notify_input_closed();

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: false,
                    waiting_run_proposal: true
                }
            );

            // Propose run completion
            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(
                vm.do_await(UnresolvedFuture::Single(handle)),
                err(is_suspended())
            );
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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::ExecuteRun(handle)
            );
            vm.propose_run_completion(
                handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(
                vm.do_await(UnresolvedFuture::Single(handle)),
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

            let RunHandle {
                replayed,
                handle: run_handle,
            } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);
            let first_sleep_handle = vm
                .sys_sleep(Default::default(), Duration::ZERO, None)
                .unwrap();

            // await any(run, first_sleep), we're still replaying here!
            assert_eq!(
                vm.do_await(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(run_handle),
                    UnresolvedFuture::Single(first_sleep_handle)
                ]))
                .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert!(vm.state().is_replaying());

            // Now we try to run!
            let second_sleep_handle = vm
                .sys_sleep(Default::default(), Duration::ZERO, None)
                .unwrap();
            assert!(vm.state().is_processing());
            assert_that!(
                vm.do_await(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(run_handle),
                    UnresolvedFuture::Single(second_sleep_handle)
                ])),
                ok(eq(AwaitResponse::ExecuteRun(run_handle)))
            );

            vm.propose_run_completion(
                run_handle,
                RunExitResult::Success(Bytes::from_static(b"123")),
                RetryPolicy::default(),
            )
            .unwrap();

            assert_that!(
                vm.do_await(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(run_handle),
                    UnresolvedFuture::Single(second_sleep_handle)
                ])),
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
    // Cancel signal wraps the original future: FirstCompleted([original, cancel])
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: empty(),
                waiting_signals: eq(vec![1]),
                waiting_named_signals: empty(),
                nested_futures: elements_are![pat!(messages::Future {
                    waiting_completions: unordered_elements_are![eq(1), eq(3)],
                    waiting_signals: empty(),
                    waiting_named_signals: empty(),
                    nested_futures: empty(),
                    combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
                })],
                combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
            }))
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

            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(replayed);
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );

            // We should now get the side effect result
            let result = vm.take_notification(handle).unwrap().unwrap();
            assert2::assert!(let Value::Success(s) = result);

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
fn replay_with_completion_followed_by_another_command_is_replayed() {
    let mut output = VMTestCase::new()
        .input(start_message(4))
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
        .input(RunCompletionNotificationMessage {
            completion_id: 1,
            result: Some(run_completion_notification_message::Result::Value(
                Bytes::from_static(b"123").into(),
            )),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            // We're still replaying and the run completion is already buffered,
            // so the closure must NOT be executed.
            let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(replayed);
            assert!(vm.state().is_replaying());

            // Replay the trailing command too.
            let _sleep_handle = vm
                .sys_sleep(Default::default(), Duration::ZERO, None)
                .unwrap();

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            let result = vm.take_notification(handle).unwrap().unwrap();
            assert2::assert!(let Value::Success(s) = result);

            vm.sys_write_output(NonEmptyValue::Success(s), PayloadOptions::default())
                .unwrap();
            vm.sys_end().unwrap();
        });

    // The run was replayed: no RunCommand nor ProposeRunCompletion is emitted.
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

            let RunHandle { replayed, .. } = vm.sys_run("my-side-effect".to_owned()).unwrap();
            assert!(!replayed);

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

// Tests for the protocol v7 ProposeRunCompletionAck flow.
mod v7_with_ack {
    use super::*;

    use test_log::test;

    #[test]
    fn propose_then_receive_ack_completes_with_success() {
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

                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::ExecuteRun(handle)
                );

                vm.propose_run_completion(
                    handle,
                    RunExitResult::Success(Bytes::from_static(b"result")),
                    RetryPolicy::default(),
                )
                .unwrap();

                // Still waiting for the ack from the runtime
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                // Runtime sends the ack — result was cached at proposal time
                vm.notify_input(
                    encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }),
                );

                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::AnyCompleted
                );
                let result = vm.take_notification(handle).unwrap().unwrap();
                assert2::assert!(let Value::Success(s) = result);

                vm.sys_write_output(NonEmptyValue::Success(s), PayloadOptions::default())
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
        let (propose_header, propose_msg) = output
            .next_with_header_decoded::<ProposeRunCompletionMessage>()
            .unwrap();
        assert_eq!(propose_header.requested_ack(), Some(true));
        assert_eq!(
            propose_msg,
            ProposeRunCompletionMessage {
                result_completion_id: 1,
                result: Some(propose_run_completion_message::Result::Value(
                    Bytes::from_static(b"result")
                )),
            }
        );
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(messages::Future {
                    waiting_completions: eq(vec![1]),
                    waiting_signals: eq(vec![1]),
                    combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"result")
        );
        output.next_decoded::<EndMessage>().unwrap();
        assert_eq!(output.next(), None);
    }

    #[test]
    fn propose_then_receive_ack_completes_with_failure() {
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

                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::ExecuteRun(handle)
                );

                vm.propose_run_completion(
                    handle,
                    RunExitResult::TerminalFailure(TerminalFailure {
                        code: 500,
                        message: "side-effect-error".to_string(),
                        metadata: vec![],
                    }),
                    RetryPolicy::default(),
                )
                .unwrap();

                // Runtime sends the ack
                vm.notify_input(
                    encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }),
                );

                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::AnyCompleted
                );
                let result = vm.take_notification(handle).unwrap().unwrap();
                assert2::assert!(let Value::Failure(f) = result);

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
        let (propose_header, propose_msg) = output
            .next_with_header_decoded::<ProposeRunCompletionMessage>()
            .unwrap();
        assert_eq!(propose_header.requested_ack(), Some(true));
        assert_eq!(
            propose_msg,
            ProposeRunCompletionMessage {
                result_completion_id: 1,
                result: Some(propose_run_completion_message::Result::Failure(Failure {
                    code: 500,
                    message: "side-effect-error".to_string(),
                    metadata: vec![],
                })),
            }
        );
        // No AwaitingOnMessage: do_await was never called between propose and ack
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_failure(500, "side-effect-error")
        );
        output.next_decoded::<EndMessage>().unwrap();
        assert_eq!(output.next(), None);
    }

    #[test]
    fn ack_with_unknown_completion_id_errors() {
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

                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::ExecuteRun(handle)
                );

                vm.propose_run_completion(
                    handle,
                    RunExitResult::Success(Bytes::from_static(b"result")),
                    RetryPolicy::default(),
                )
                .unwrap();

                // Runtime sends ack with a completion_id we never proposed
                vm.notify_input(
                    encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 99 }),
                );
            });

        let _ = output.next_decoded::<RunCommandMessage>().unwrap();
        let (propose_header, _) = output
            .next_with_header_decoded::<ProposeRunCompletionMessage>()
            .unwrap();
        assert_eq!(propose_header.requested_ack(), Some(true));
        // No AwaitingOnMessage: do_await was not called between propose and the bad ack
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(crate::vm::errors::codes::PROTOCOL_VIOLATION.code() as u32),
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn ack_during_replay_is_unexpected() {
        // Feed the ack while the VM is still replaying (known_entries=3 but only 2 replayed).
        // The third entry in the stream is ProposeRunCompletionAckMessage, which is illegal
        // during replay.
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 3,
                partial_state: false,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .input(RunCommandMessage {
                result_completion_id: 1,
                name: "my-side-effect".to_owned(),
            })
            // This is fed as if it were the third journal entry, but it is not a valid
            // journal entry — the runtime must never send ProposeRunCompletionAckMessage
            // during the replay phase.
            .input(ProposeRunCompletionAckMessage { completion_id: 1 });

        // The VM should have transitioned to an error state. Drain any buffered output
        // without going through run_without_closing_input (which would assert ready-to-execute).
        let mut decoder =
            crate::service_protocol::Decoder::new(Version::maximum_supported_version());
        while let TakeOutputResult::Buffer(b) = output.vm.take_output() {
            decoder.push(b);
        }
        let mut raw_output: Vec<_> =
            std::iter::from_fn(|| decoder.consume_next().unwrap()).collect();

        let last = raw_output.pop().unwrap();
        let error_msg = last.decode_to::<ErrorMessage>(0).unwrap();
        assert_ne!(error_msg.code, 0);
    }
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
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();

                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
                vm.propose_run_completion(
                    handle,
                    RunExitResult::RetryableFailure {
                        error: Error::internal("my-error").with_stacktrace("my-stacktrace"),
                        attempt_duration,
                    },
                    retry_policy,
                )
                .unwrap();

                vm.notify_input(encoder.encode(&RunCompletionNotificationMessage {
                    completion_id: 1,
                    result: Some(run_completion_notification_message::Result::Failure(
                        Failure {
                            code: 500,
                            message: "my-error".to_string(),
                            metadata: vec![],
                        },
                    )),
                }));
                vm.notify_input_closed();

                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::AnyCompleted
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
                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                on_max_attempts: OnMaxAttempts::FailAsTerminal,
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
                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);

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
                            on_max_attempts: OnMaxAttempts::FailAsTerminal,
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

    fn test_should_pause_on_exhaustion(
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
            .run(|vm| {
                vm.sys_input().unwrap();
                let RunHandle { replayed, handle } =
                    vm.sys_run("my-side-effect".to_owned()).unwrap();
                assert!(!replayed);
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
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(500),
                message: eq("my-error".to_string()),
                stacktrace: eq("my-stacktrace".to_string()),
                should_pause: eq(true),
                next_retry_delay: eq(None),
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn exit_with_retryable_error_fixed_delay_pause_on_max_attempts() {
        test_should_pause_on_exhaustion(
            9,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::FixedDelay {
                interval: Some(Duration::from_secs(1)),
                max_attempts: Some(10),
                max_duration: None,
                on_max_attempts: OnMaxAttempts::Pause,
            },
        );
    }

    #[test]
    fn exit_with_retryable_error_fixed_delay_pause_on_max_duration() {
        test_should_pause_on_exhaustion(
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::FixedDelay {
                interval: Some(Duration::from_secs(1)),
                max_attempts: None,
                max_duration: Some(Duration::from_secs(2)),
                on_max_attempts: OnMaxAttempts::Pause,
            },
        );
    }

    #[test]
    fn exit_with_retryable_error_exponential_pause_on_max_attempts() {
        test_should_pause_on_exhaustion(
            0,
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryPolicy::Exponential {
                initial_interval: Duration::from_secs(1),
                factor: 1.0,
                max_attempts: Some(0),
                max_duration: None,
                max_interval: None,
                on_max_attempts: OnMaxAttempts::Pause,
            },
        );
    }

    #[test]
    fn propose_run_completion_with_pause_policy_on_v6_returns_unsupported_feature() {
        let mut vm = CoreVM::mock_init(Version::V6);
        let encoder = Encoder::new(Version::V6);
        vm.notify_input(encoder.encode(&start_message(1)));
        vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
        vm.notify_input_closed();
        vm.sys_input().unwrap();
        let RunHandle { replayed, handle } = vm.sys_run("my-side-effect".to_owned()).unwrap();
        assert!(!replayed);

        let err = vm
            .propose_run_completion(
                handle,
                RunExitResult::RetryableFailure {
                    error: Error::internal("my-error"),
                    attempt_duration: Duration::from_millis(0),
                },
                RetryPolicy::FixedDelay {
                    interval: Some(Duration::from_secs(1)),
                    max_attempts: Some(1),
                    max_duration: None,
                    on_max_attempts: OnMaxAttempts::Pause,
                },
            )
            .unwrap_err();

        assert_eq!(
            err.code,
            crate::vm::errors::codes::UNSUPPORTED_FEATURE.code()
        );
    }
}
