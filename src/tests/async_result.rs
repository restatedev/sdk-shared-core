use super::*;

use crate::service_protocol::messages::*;
use crate::{PayloadOptions, Value};

use test_log::test;

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greeter".to_string(),
        key: None,
        idempotency_key: None,
        headers: Vec::new(),
    }
}

#[test]
fn dont_await_call() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputCommandMessage::default())
        .run(|vm| {
            vm.sys_input().unwrap();

            let _ = vm
                .sys_call(
                    greeter_target(),
                    Bytes::from_static(b"Francesco"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"Whatever")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        CallCommandMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..Default::default()
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"Whatever")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn dont_await_call_dont_notify_input_closed() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputCommandMessage::default())
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();
            let _ = vm
                .sys_call(
                    greeter_target(),
                    Bytes::from_static(b"Francesco"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"Whatever")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        CallCommandMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..Default::default()
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"Whatever")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

mod do_progress {
    use super::*;

    use test_log::test;

    #[test]
    fn await_twice_the_same_handle() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();

                let (_, h) = vm.sys_awakeable().unwrap();

                assert_eq!(
                    vm.do_progress(UnresolvedFuture::Single(h)).unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );
                assert_eq!(
                    vm.do_progress(UnresolvedFuture::Single(h)).unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                vm.notify_input_closed();

                assert_that!(
                    vm.do_progress(UnresolvedFuture::Single(h)),
                    err(is_suspended())
                );
            });

        // Two do_progress calls returned WaitingExternalProgress, each emits AwaitingOnMessage
        // The future is FirstCompleted([Single(signal_17), Single(cancel_signal)])
        for _ in 0..2 {
            assert_that!(
                output.next_decoded::<AwaitingOnMessage>().unwrap(),
                pat!(AwaitingOnMessage {
                    awaiting_on: some(pat!(Future {
                        waiting_signals: unordered_elements_are![eq(17), eq(1)],
                        waiting_completions: empty(),
                        waiting_named_signals: empty(),
                        nested_futures: empty(),
                        combinator_type: eq(CombinatorType::FirstCompleted as i32)
                    })),
                    executing_side_effects: eq(false)
                })
            );
        }
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_signal(17)
        );
        assert_eq!(output.next(), None);
    }

    /// AwaitingOnMessage should reflect the partially resolved future, not the original.
    /// all_completed(h1, h2): h1 completes in the queue, h2 still pending.
    /// The AwaitingOn should contain only h2 (the reduced future).
    #[test]
    fn awaiting_on_reflects_partially_resolved_future() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();

                let (_, h1) = vm.sys_awakeable().unwrap(); // signal 17
                let (_, h2) = vm.sys_awakeable().unwrap(); // signal 18

                // Send completion for h1 before calling do_progress
                vm.notify_input(encoder.encode(&SignalNotificationMessage {
                    signal_id: Some(signal_notification_message::SignalId::Idx(17)),
                    result: Some(signal_notification_message::Result::Value(
                        Bytes::from_static(b"v1").into(),
                    )),
                }));

                // do_progress with all_completed(h1, h2):
                // h1 resolves from queue, h2 still pending → WaitingExternalProgress
                assert_eq!(
                    vm.do_progress(UnresolvedFuture::AllCompleted(vec![
                        UnresolvedFuture::Single(h1),
                        UnresolvedFuture::Single(h2),
                    ]))
                    .unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                vm.notify_input_closed();
                assert_that!(
                    vm.do_progress(UnresolvedFuture::AllCompleted(vec![
                        UnresolvedFuture::Single(h1),
                        UnresolvedFuture::Single(h2),
                    ])),
                    err(is_suspended())
                );
            });

        // AwaitingOn should have the REDUCED future: only h2 remaining
        // The cancel signal wraps it: FirstCompleted([AllCompleted([Single(signal_18)]), Single(cancel)])
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_completions: empty(),
                    waiting_signals: eq(vec![1]),
                    waiting_named_signals: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: eq(vec![18]),
                        waiting_completions: empty(),
                        nested_futures: empty(),
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );
        // Suspension has the same reduced future (only h2 remaining)
        // Cancel signal wraps: FirstCompleted([AllCompleted([Single(signal_18)]), Single(cancel)])
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            pat!(SuspensionMessage {
                awaiting_on: some(pat!(Future {
                    waiting_completions: empty(),
                    waiting_signals: eq(vec![1]),
                    waiting_named_signals: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: eq(vec![18]),
                        waiting_completions: empty(),
                        nested_futures: empty(),
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                }))
            })
        );
        assert_eq!(output.next(), None);
    }

    /// AwaitingOnMessage should reflect the normalized future.
    /// asff(h1, unknown(h2)) normalizes to unknown(h1, h2).
    #[test]
    fn awaiting_on_reflects_normalized_future() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();

                let (_, h1) = vm.sys_awakeable().unwrap(); // signal 17
                let (_, h2) = vm.sys_awakeable().unwrap(); // signal 18

                // do_progress with asff(h1, unknown(h2))
                // Normalization: asff extracts unknown → unknown(h1, h2)
                assert_eq!(
                    vm.do_progress(UnresolvedFuture::AllSucceededOrFirstFailed(vec![
                        UnresolvedFuture::Single(h1),
                        UnresolvedFuture::Unknown(vec![UnresolvedFuture::Single(h2)]),
                    ]))
                    .unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                vm.notify_input_closed();
                assert_that!(
                    vm.do_progress(UnresolvedFuture::AllSucceededOrFirstFailed(vec![
                        UnresolvedFuture::Single(h1),
                        UnresolvedFuture::Unknown(vec![UnresolvedFuture::Single(h2)]),
                    ])),
                    err(is_suspended())
                );
            });

        // Cancel wraps BEFORE normalization: FirstCompleted([asff(h1, unknown(h2)), cancel])
        // Then normalization extracts unknown from asff (inside FirstCompleted context inherited from cancel wrapper):
        // Wait — the cancel wrapper is FirstCompleted, not fsaf/asff. So extract=false for the wrapper.
        // But asff IS fsaf/asff, so it sets extract=true for its own children.
        // asff(h1, unknown(h2)): extract unknown(h2) → unknown_nodes=[h2], asff collapses to h1.
        // Root normalize: unknown_nodes=[h2] → wrap: Unknown([FirstCompleted([h1, cancel]), h2])
        // Serialized: root=CombinatorUnknown, h2 inlined, nested FirstCompleted with h1+cancel
        // AwaitingOn: root=Unknown, h2 inlined, nested FirstCompleted(h1, cancel)
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![18]),
                    waiting_completions: empty(),
                    waiting_named_signals: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: unordered_elements_are![eq(17), eq(1)],
                        waiting_completions: empty(),
                        nested_futures: empty(),
                        combinator_type: eq(CombinatorType::FirstCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
                })),
                executing_side_effects: eq(false)
            })
        );
        // Suspension has the same normalized form
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            pat!(SuspensionMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![18]),
                    waiting_completions: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: unordered_elements_are![eq(17), eq(1)],
                        combinator_type: eq(CombinatorType::FirstCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::CombinatorUnknown as i32)
                }))
            })
        );
        assert_eq!(output.next(), None);
    }

    /// Multiple do_progress calls with progressively shrinking future.
    /// all_completed(h1, h2, h3): first call nothing ready, second call h1 arrives,
    /// third call h2 arrives. Each AwaitingOn should reflect the current state.
    #[test]
    fn awaiting_on_shrinks_across_calls() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, encoder| {
                vm.sys_input().unwrap();

                let (_, h1) = vm.sys_awakeable().unwrap(); // signal 17
                let (_, h2) = vm.sys_awakeable().unwrap(); // signal 18
                let (_, h3) = vm.sys_awakeable().unwrap(); // signal 19

                let fut = || {
                    UnresolvedFuture::AllCompleted(vec![
                        UnresolvedFuture::Single(h1),
                        UnresolvedFuture::Single(h2),
                        UnresolvedFuture::Single(h3),
                    ])
                };

                // First call: nothing ready → WaitingExternalProgress, AwaitingOn has all 3
                assert_eq!(
                    vm.do_progress(fut()).unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                // h1 arrives
                vm.notify_input(encoder.encode(&SignalNotificationMessage {
                    signal_id: Some(signal_notification_message::SignalId::Idx(17)),
                    result: Some(signal_notification_message::Result::Value(
                        Bytes::from_static(b"v1").into(),
                    )),
                }));

                // Second call: h1 resolves, h2+h3 still pending → WaitingExternalProgress
                assert_eq!(
                    vm.do_progress(fut()).unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                // h2 arrives
                vm.notify_input(encoder.encode(&SignalNotificationMessage {
                    signal_id: Some(signal_notification_message::SignalId::Idx(18)),
                    result: Some(signal_notification_message::Result::Value(
                        Bytes::from_static(b"v2").into(),
                    )),
                }));

                // Third call: h1+h2 resolved, h3 still pending → WaitingExternalProgress
                assert_eq!(
                    vm.do_progress(fut()).unwrap(),
                    DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal: false
                    }
                );

                vm.notify_input_closed();
                assert_that!(vm.do_progress(fut()), err(is_suspended()));
            });

        // First AwaitingOn: all 3 handles
        // Cancel wraps: FirstCompleted([AllCompleted([17, 18, 19]), Single(cancel)])
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![1]),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: unordered_elements_are![eq(17), eq(18), eq(19)],
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );

        // Second AwaitingOn: h1 resolved, only h2+h3 remain
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![1]),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: unordered_elements_are![eq(18), eq(19)],
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );

        // Third AwaitingOn: h1+h2 resolved, only h3 remains
        // AllCompleted([h3]) stays as-is (no collapse during resolution, only normalization)
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![1]),
                    waiting_completions: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: eq(vec![19]),
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );

        // Suspension with only h3, same structure
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            pat!(SuspensionMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: eq(vec![1]),
                    waiting_completions: empty(),
                    nested_futures: elements_are![pat!(Future {
                        waiting_signals: eq(vec![19]),
                        combinator_type: eq(CombinatorType::AllCompleted as i32)
                    })],
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                }))
            })
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
            .sys_call(
                greeter_target(),
                Bytes::from_static(b"Francesco"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();
        let h2 = vm
            .sys_call(
                greeter_target(),
                Bytes::from_static(b"Till"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

        if vm
            .do_progress(UnresolvedFuture::Single(h2.call_notification_handle))
            .is_err_and(|e| e.is_suspended_error())
        {
            assert_that!(
                vm.take_notification(h2.call_notification_handle),
                err(is_closed())
            );
            return;
        }
        assert2::assert!(
            let Some(Value::Success(h2_value)) =
                vm.take_notification(h2.call_notification_handle).unwrap()
        );

        vm.sys_state_set("A2".to_owned(), h2_value.clone(), PayloadOptions::default())
            .unwrap();

        if vm
            .do_progress(UnresolvedFuture::Single(h1.call_notification_handle))
            .is_err_and(|e| e.is_suspended_error())
        {
            assert_that!(
                vm.take_notification(h1.call_notification_handle),
                err(is_closed())
            );
            return;
        }
        assert2::assert!(
            let Some(Value::Success(h1_value)) =
                vm.take_notification(h1.call_notification_handle).unwrap()
        );

        vm.sys_write_output(
            NonEmptyValue::Success(Bytes::from([&h1_value[..], b"-", &h2_value[..]].concat())),
            PayloadOptions::default(),
        )
        .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn none_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(4)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn a1_and_a2_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallInvocationIdCompletionNotificationMessage {
                completion_id: 1,
                invocation_id: "a1".to_string(),
            })
            .input(CallInvocationIdCompletionNotificationMessage {
                completion_id: 3,
                invocation_id: "a2".to_string(),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FRANCESCO-TILL")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn a2_and_a1_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FRANCESCO-TILL")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn only_a2_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(2)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn only_a1_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(4)
        );

        assert_eq!(output.next(), None);
    }
}

mod combinators {
    use super::*;

    use test_log::test;

    #[test]
    fn replay_with_combinator_and_entry_afterwards() {
        let mut output = VMTestCase::new()
            .input(start_message(5))
            .input(input_entry_message(b"my-data"))
            // Two sleep are created
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 1,
                ..Default::default()
            })
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 2,
                ..Default::default()
            })
            // Only one of them completes
            .input(SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(Default::default()),
            })
            // Another sleep here
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 3,
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                // Simulating the user code should be:
                //
                // val a = sleep()
                // val b = sleep()
                // await any(a, b)
                // val c = sleep()
                // await c

                let a_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();
                let b_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();

                // Transition should work fine here!
                assert_that!(
                    vm.do_progress(UnresolvedFuture::FirstCompleted(vec![
                        UnresolvedFuture::Single(a_handle),
                        UnresolvedFuture::Single(b_handle)
                    ])),
                    ok(eq(DoProgressResponse::AnyCompleted))
                );
                assert!(!vm.is_completed(a_handle));
                assert!(vm.is_completed(b_handle));

                // Code moves on to c = sleep() and suspends
                let c_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();
                assert_that!(
                    vm.do_progress(UnresolvedFuture::Single(c_handle)),
                    err(is_suspended())
                );
            });

        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(3)
        );
        assert_eq!(output.next(), None);
    }
}
