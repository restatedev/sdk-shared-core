use super::*;

use crate::service_protocol::messages::start_message::StateEntry;
use crate::service_protocol::messages::ErrorBehavior;
use crate::service_protocol::messages::{
    get_lazy_state_completion_notification_message, get_state_ephemeral_notification_message,
    propose_run_completion_message, run_completion_notification_message,
    ClearAllStateCommandMessage, ClearStateCommandMessage, EndMessage, GetLazyStateCommandMessage,
    GetLazyStateCompletionNotificationMessage, GetStateEphemeralCommandMessage,
    GetStateEphemeralNotificationMessage, ProposeRunCompletionAckMessage,
    ProposeRunCompletionMessage, RunCommandMessage, RunCompletionNotificationMessage,
    SetStateCommandMessage,
};
use crate::vm::errors::codes;
use crate::vm::errors::{
    EphemeralStateGetWithClosedInput, EPHEMERAL_COMMAND_DURING_REPLAY,
    INPUT_CLOSED_WHILE_WAITING_EPHEMERAL_NOTIFICATIONS,
};

fn start_with_state(partial_state: bool, state: &[(&'static str, &'static str)]) -> StartMessage {
    StartMessage {
        known_entries: 1,
        partial_state,
        state_map: state
            .iter()
            .map(|(k, v)| StateEntry {
                key: Bytes::from_static(k.as_bytes()),
                value: Bytes::from_static(v.as_bytes()),
            })
            .collect(),
        ..start_message(1)
    }
}

fn get_state_notification(
    completion_id: u32,
    value: Option<&'static str>,
) -> GetStateEphemeralNotificationMessage {
    GetStateEphemeralNotificationMessage {
        ephemeral_completion_id: completion_id,
        result: Some(match value {
            None => get_state_ephemeral_notification_message::Result::Void(Default::default()),
            Some(v) => get_state_ephemeral_notification_message::Result::Value(
                Bytes::from_static(v.as_bytes()).into(),
            ),
        }),
    }
}

/// Start a run, and assert the VM wants to execute it.
fn start_run(vm: &mut CoreVM) -> NotificationHandle {
    let RunHandle { replayed, handle } = vm.sys_run("project:STATE".to_owned()).unwrap();
    assert!(!replayed);
    assert_eq!(
        vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
        AwaitResponse::ExecuteRun(handle)
    );
    handle
}

/// Await the notification of the given ephemeral command, assuming it's the only ready one, and take it.
fn await_and_take(
    vm: &mut CoreVM,
    awaiting: NotificationHandle,
    completion_id: EphemeralCompletionId,
) -> Value {
    assert_eq!(
        vm.do_await(UnresolvedFuture::Single(awaiting)).unwrap(),
        AwaitResponse::EphemeralNotificationReady(completion_id)
    );
    let value = vm
        .take_ephemeral_notification(completion_id)
        .unwrap()
        .unwrap();
    // Can't take it twice
    assert_eq!(vm.take_ephemeral_notification(completion_id).unwrap(), None);
    value
}

fn assert_run_command(output: &mut OutputIterator) {
    assert_eq!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        RunCommandMessage {
            result_completion_id: 1,
            name: "project:STATE".to_owned(),
        }
    );
}

mod local {
    use super::*;
    use test_log::test;

    #[test]
    fn complete_eager_state() {
        let mut output = VMTestCase::new()
            .input(start_with_state(false, &[("STATE", "Francesco")]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                assert_eq!(u32::from(id), 1);
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"Francesco"))
                );

                // Complete state, missing key is void
                let id = vm.ephemeral_state_get("OTHER".to_owned()).unwrap();
                assert_eq!(u32::from(id), 2);
                assert_eq!(await_and_take(vm, run, id), Value::Void);
            });

        assert_run_command(&mut output);
        // No GetStateEphemeralCommandMessage
        assert_eq!(output.next(), None);
    }

    #[test]
    fn partial_eager_state_with_key() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[("STATE", "Francesco")]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"Francesco"))
                );
            });

        assert_run_command(&mut output);
        assert_eq!(output.next(), None);
    }

    #[test]
    fn served_with_closed_input() {
        // E.g. request/response mode, all good as long as the key is known
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[("STATE", "Francesco")]))
            .input(input_entry_message(b"my-data"))
            .run(|vm| {
                vm.sys_input().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"Francesco"))
                );
            });

        assert_run_command(&mut output);
        assert_eq!(output.next(), None);
    }

    #[test]
    fn after_set_and_clear() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[("B", "b")]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                vm.sys_state_set(
                    "A".to_owned(),
                    Bytes::from_static(b"a"),
                    PayloadOptions::default(),
                )
                .unwrap();
                vm.sys_state_clear("B".to_owned()).unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("A".to_owned()).unwrap();
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"a"))
                );
                let id = vm.ephemeral_state_get("B".to_owned()).unwrap();
                assert_eq!(await_and_take(vm, run, id), Value::Void);
            });

        output.next_decoded::<SetStateCommandMessage>().unwrap();
        output.next_decoded::<ClearStateCommandMessage>().unwrap();
        assert_run_command(&mut output);
        assert_eq!(output.next(), None);
    }

    #[test]
    fn after_clear_all() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                vm.sys_state_clear_all().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("UNKNOWN".to_owned()).unwrap();
                assert_eq!(await_and_take(vm, run, id), Value::Void);
            });

        output
            .next_decoded::<ClearAllStateCommandMessage>()
            .unwrap();
        assert_run_command(&mut output);
        assert_eq!(output.next(), None);
    }

    #[test]
    fn after_replayed_set_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                known_entries: 2,
                ..start_with_state(true, &[])
            })
            .input(input_entry_message(b"my-data"))
            .input(SetStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                value: Some(Bytes::from_static(b"Francesco").into()),
                ..Default::default()
            })
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                vm.sys_state_set(
                    "STATE".to_owned(),
                    Bytes::from_static(b"Francesco"),
                    PayloadOptions::default(),
                )
                .unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"Francesco"))
                );
            });

        assert_run_command(&mut output);
        assert_eq!(output.next(), None);
    }
}

mod remote {
    use super::*;
    use test_log::test;

    #[test]
    fn get_state_then_propose_projection() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                assert_eq!(u32::from(id), 1);

                // Not ready yet
                assert_eq!(vm.take_ephemeral_notification(id).unwrap(), None);
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(run)).unwrap(),
                    AwaitResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: true
                    }
                );

                vm.notify_input(encoder.encode(&get_state_notification(1, Some("Francesco"))));
                let value = await_and_take(vm, run, id);
                assert2::assert!(let Value::Success(s) = value);

                // Projection
                vm.propose_run_completion(
                    run,
                    RunExitResult::Success(Bytes::from(s.len().to_string())),
                    RetryPolicy::default(),
                )
                .unwrap();
                vm.notify_input(
                    encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }),
                );
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(run)).unwrap(),
                    AwaitResponse::AnyCompleted
                );
                assert2::assert!(let Some(Value::Success(projected)) = vm.take_notification(run).unwrap());

                vm.sys_write_output(NonEmptyValue::Success(projected), PayloadOptions::default())
                    .unwrap();
                vm.sys_end().unwrap();
            });

        assert_run_command(&mut output);
        assert_eq!(
            output
                .next_decoded::<GetStateEphemeralCommandMessage>()
                .unwrap(),
            GetStateEphemeralCommandMessage {
                ephemeral_completion_id: 1,
                key: Bytes::from_static(b"STATE"),
            }
        );
        // The projection result is the only thing recorded
        assert_eq!(
            output
                .next_decoded::<ProposeRunCompletionMessage>()
                .unwrap(),
            ProposeRunCompletionMessage {
                result_completion_id: 1,
                result: Some(propose_run_completion_message::Result::Value(
                    Bytes::from_static(b"9")
                )),
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"9")
        );
        output.next_decoded::<EndMessage>().unwrap();
        assert_eq!(output.next(), None);
    }

    #[test]
    fn void_notification() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();
                let run = start_run(vm);

                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                vm.notify_input(encoder.encode(&get_state_notification(1, None)));
                assert_eq!(await_and_take(vm, run, id), Value::Void);
            });

        assert_run_command(&mut output);
        output
            .next_decoded::<GetStateEphemeralCommandMessage>()
            .unwrap();
        assert_eq!(output.next(), None);
    }

    #[test]
    fn takes_precedence_and_ids_are_independent_of_completion_ids() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();

                let h1 = vm
                    .sys_state_get("A".to_owned(), PayloadOptions::default())
                    .unwrap();
                let u1 = vm.ephemeral_state_get("B".to_owned()).unwrap();
                let h2 = vm
                    .sys_state_get("C".to_owned(), PayloadOptions::default())
                    .unwrap();
                let u2 = vm.ephemeral_state_get("D".to_owned()).unwrap();
                assert_eq!(u32::from(u1), 1);
                assert_eq!(u32::from(u2), 2);

                vm.notify_input(encoder.encode(&GetLazyStateCompletionNotificationMessage {
                    completion_id: 1,
                    result: Some(
                        get_lazy_state_completion_notification_message::Result::Value(
                            Bytes::from_static(b"a").into(),
                        ),
                    ),
                }));
                vm.notify_input(encoder.encode(&get_state_notification(2, Some("d"))));
                vm.notify_input(encoder.encode(&get_state_notification(1, Some("b"))));

                // Both ephemeral notifications come first, in any order
                let mut ready = vec![];
                for _ in 0..2 {
                    let AwaitResponse::EphemeralNotificationReady(id) =
                        vm.do_await(UnresolvedFuture::Single(h1)).unwrap()
                    else {
                        panic!("Expected EphemeralNotificationReady")
                    };
                    let value = vm.take_ephemeral_notification(id).unwrap().unwrap();
                    ready.push((u32::from(id), value));
                }
                ready.sort_by_key(|(id, _)| *id);
                assert_eq!(
                    ready,
                    vec![
                        (1, Value::Success(Bytes::from_static(b"b"))),
                        (2, Value::Success(Bytes::from_static(b"d")))
                    ]
                );

                // Then the notification
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(h1)).unwrap(),
                    AwaitResponse::AnyCompleted
                );
                assert_eq!(
                    vm.take_notification(h1).unwrap(),
                    Some(Value::Success(Bytes::from_static(b"a")))
                );
                assert!(!vm.is_completed(h2));
            });

        assert_eq!(
            output
                .next_decoded::<GetLazyStateCommandMessage>()
                .unwrap()
                .result_completion_id,
            1
        );
        assert_eq!(
            output
                .next_decoded::<GetStateEphemeralCommandMessage>()
                .unwrap(),
            GetStateEphemeralCommandMessage {
                ephemeral_completion_id: 1,
                key: Bytes::from_static(b"B"),
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetLazyStateCommandMessage>()
                .unwrap()
                .result_completion_id,
            2
        );
        assert_eq!(
            output
                .next_decoded::<GetStateEphemeralCommandMessage>()
                .unwrap(),
            GetStateEphemeralCommandMessage {
                ephemeral_completion_id: 2,
                key: Bytes::from_static(b"D"),
            }
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn does_not_affect_replay_of_the_run() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                known_entries: 3,
                ..start_with_state(true, &[])
            })
            .input(input_entry_message(b"my-data"))
            .input(RunCommandMessage {
                result_completion_id: 1,
                name: "project:STATE".to_owned(),
            })
            .input(RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Value(
                    Bytes::from_static(b"9").into(),
                )),
            })
            .run(|vm| {
                vm.sys_input().unwrap();
                let RunHandle { replayed, handle } =
                    vm.sys_run("project:STATE".to_owned()).unwrap();
                assert!(replayed);
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                    AwaitResponse::AnyCompleted
                );
                assert2::assert!(let Some(Value::Success(projected)) = vm.take_notification(handle).unwrap());
                vm.sys_write_output(NonEmptyValue::Success(projected), PayloadOptions::default())
                    .unwrap();
                vm.sys_end().unwrap();
            });

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"9")
        );
        output.next_decoded::<EndMessage>().unwrap();
        assert_eq!(output.next(), None);
    }
}

mod failures {
    use super::*;
    use test_log::test;

    fn expect_protocol_violation(notification: GetStateEphemeralNotificationMessage) {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();
                let run = start_run(vm);
                vm.ephemeral_state_get("STATE".to_owned()).unwrap();

                vm.notify_input(encoder.encode(&notification));
                assert_that!(
                    vm.do_await(UnresolvedFuture::Single(run)),
                    err(pat!(Error {
                        code: eq(codes::PROTOCOL_VIOLATION.code())
                    }))
                );
            });

        assert_run_command(&mut output);
        output
            .next_decoded::<GetStateEphemeralCommandMessage>()
            .unwrap();
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(codes::PROTOCOL_VIOLATION.code() as u32),
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn notification_with_unknown_id_is_ignored() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();
                let run = start_run(vm);
                let id = vm.ephemeral_state_get("STATE".to_owned()).unwrap();

                // Unknown id, ignored
                vm.notify_input(encoder.encode(&get_state_notification(99, Some("Unknown"))));
                assert_eq!(
                    vm.do_await(UnresolvedFuture::Single(run)).unwrap(),
                    AwaitResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: true
                    }
                );

                // The in-flight command still completes
                vm.notify_input(encoder.encode(&get_state_notification(1, Some("Francesco"))));
                assert_eq!(
                    await_and_take(vm, run, id),
                    Value::Success(Bytes::from_static(b"Francesco"))
                );
            });

        assert_run_command(&mut output);
        output
            .next_decoded::<GetStateEphemeralCommandMessage>()
            .unwrap();
        // No error message
        assert_eq!(output.next(), None);
    }

    #[test]
    fn notification_without_result() {
        expect_protocol_violation(GetStateEphemeralNotificationMessage {
            ephemeral_completion_id: 1,
            result: None,
        });
    }

    #[test]
    fn notification_after_closed_is_ignored() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();
                vm.ephemeral_state_get("STATE".to_owned()).unwrap();
                vm.sys_write_output(
                    NonEmptyValue::Success(Bytes::from_static(b"done")),
                    PayloadOptions::default(),
                )
                .unwrap();
                vm.sys_end().unwrap();

                vm.notify_input(encoder.encode(&get_state_notification(1, Some("Francesco"))));

                // Still closed, not failed
                assert_that!(vm.is_ready_to_execute(), err(is_closed()));
            });

        output
            .next_decoded::<GetStateEphemeralCommandMessage>()
            .unwrap();
        output.next_decoded::<OutputCommandMessage>().unwrap();
        output.next_decoded::<EndMessage>().unwrap();
        assert_eq!(output.next(), None);
    }

    #[test]
    fn input_already_closed_and_unknown_key() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run(|vm| {
                vm.sys_input().unwrap();
                start_run(vm);
                assert_that!(
                    vm.ephemeral_state_get("STATE".to_owned()),
                    err(eq_error(
                        EphemeralStateGetWithClosedInput::new("STATE".to_owned()).into()
                    ))
                );
            });

        assert_run_command(&mut output);
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(
                EphemeralStateGetWithClosedInput::new("STATE".to_owned()).into()
            )
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn input_closed_while_in_flight_errors_instead_of_suspending() {
        let mut output = VMTestCase::new()
            .input(start_with_state(true, &[]))
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();
                let run = start_run(vm);
                vm.ephemeral_state_get("STATE".to_owned()).unwrap();

                vm.notify_input_closed();
                assert_that!(
                    vm.do_await(UnresolvedFuture::Single(run)),
                    err(eq_error(INPUT_CLOSED_WHILE_WAITING_EPHEMERAL_NOTIFICATIONS))
                );
            });

        assert_run_command(&mut output);
        output
            .next_decoded::<GetStateEphemeralCommandMessage>()
            .unwrap();
        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(INPUT_CLOSED_WHILE_WAITING_EPHEMERAL_NOTIFICATIONS)
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn unsupported_on_v7() {
        let mut output = VMTestCase::with_version(Version::V7)
            .input(start_with_state(false, &[("STATE", "Francesco")]))
            .input(input_entry_message(b"my-data"))
            .run(|vm| {
                vm.sys_input().unwrap();
                assert_that!(
                    vm.ephemeral_state_get("STATE".to_owned()),
                    err(pat!(Error {
                        code: eq(codes::UNSUPPORTED_FEATURE.code())
                    }))
                );
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(codes::UNSUPPORTED_FEATURE.code() as u32),
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn during_replay_follows_journal_mismatch_behavior() {
        let mut output = VMTestCase::with_vm_options(VMOptions {
            journal_mismatch_retry_behavior: JournalMismatchRetryBehavior::Pause,
            ..VMOptions::default()
        })
        .input(StartMessage {
            known_entries: 2,
            ..start_with_state(false, &[("STATE", "Francesco")])
        })
        .input(input_entry_message(b"my-data"))
        .input(ClearAllStateCommandMessage::default())
        .run(|vm| {
            vm.sys_input().unwrap();
            assert!(vm.ephemeral_state_get("STATE".to_owned()).is_err());
        });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            pat!(ErrorMessage {
                code: eq(codes::JOURNAL_MISMATCH.code() as u32),
                behavior: eq(i32::from(ErrorBehavior::Pause)),
            })
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn during_replay() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                known_entries: 2,
                ..start_with_state(false, &[("STATE", "Francesco")])
            })
            .input(input_entry_message(b"my-data"))
            .input(ClearAllStateCommandMessage::default())
            .run(|vm| {
                vm.sys_input().unwrap();
                assert_that!(
                    vm.ephemeral_state_get("STATE".to_owned()),
                    err(eq_error(EPHEMERAL_COMMAND_DURING_REPLAY))
                );
            });

        assert_that!(
            output.next_decoded::<ErrorMessage>().unwrap(),
            error_message_as_error(EPHEMERAL_COMMAND_DURING_REPLAY)
        );
        assert_eq!(output.next(), None);
    }
}
