use super::*;

use crate::service_protocol::messages::*;
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::Value;
use googletest::prelude::*;
use test_log::test;

#[test]
fn call_then_get_invocation_id_then_cancel() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            // invocation id is here, let's take it and assert it
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(
                    call_handle.invocation_id_notification_handle
                ))
                .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert2::assert!(
                let Some(Value::InvocationId(invocation_id)) = vm
                    .take_notification(call_handle.invocation_id_notification_handle)
                    .unwrap()
            );
            assert_eq!(invocation_id, "my-id");

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(
                    call_handle.call_notification_handle
                ))
                .unwrap(),
                AwaitResponse::CancelSignalReceived
            );

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
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
fn call_then_cancel() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id".to_string(),
        })
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(
                    call_handle.call_notification_handle
                ))
                .unwrap(),
                AwaitResponse::CancelSignalReceived
            );

            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
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
fn call_then_cancel_without_invocation_id() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .input(cancel_signal_notification())
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            // Suspends because it's missing the invocation id to complete the cancellation
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(
                    call_handle.call_notification_handle
                )),
                err(is_suspended())
            );
        });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: eq(vec![1]),
                waiting_signals: empty(),
                nested_futures: empty(),
                waiting_named_signals: empty()
            }))
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn call_then_then_cancel_disabling_children_cancellation() {
    let mut output = VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Enabled {
            cancel_children_calls: false,
            cancel_children_one_way_calls: true,
        },
        ..VMOptions::default()
    })
    .input(start_message(1))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .run(|vm| {
        vm.sys_input().unwrap();

        let call_handle = vm
            .sys_call(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    scope: None,
                    limit_key: None,
                    headers: Vec::new(),
                },
                Bytes::new(),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

        assert_eq!(
            vm.do_await(UnresolvedFuture::Single(
                call_handle.call_notification_handle
            ))
            .unwrap(),
            AwaitResponse::CancelSignalReceived
        );

        vm.sys_end().unwrap();
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
        })
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn disabled_implicit_cancellation() {
    let mut output = VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Disabled,
        ..VMOptions::default()
    })
    .input(start_message(1))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .run(|vm| {
        vm.sys_input().unwrap();

        let call_handle = vm
            .sys_call(
                Target {
                    service: "MySvc".to_string(),
                    handler: "MyHandler".to_string(),
                    key: None,
                    idempotency_key: None,
                    scope: None,
                    limit_key: None,
                    headers: Vec::new(),
                },
                Bytes::new(),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

        // Just suspended
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(
                call_handle.call_notification_handle
            )),
            err(is_suspended())
        );
    });

    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("MySvc"),
            handler_name: eq("MyHandler"),
            invocation_id_notification_idx: eq(1),
            result_completion_id: eq(2),
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: eq(vec![2]),
                waiting_signals: empty(),
                nested_futures: empty(),
                waiting_named_signals: empty()
            }))
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn replay_while_cancelling() {
    let mut output = VMTestCase::new()
        .input(start_message(7))
        .input(input_entry_message(b"my-data"))
        .input(CallCommandMessage {
            service_name: "MySvc".to_string(),
            handler_name: "MyHandler".to_string(),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..CallCommandMessage::default()
        })
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "my-id-1".to_string(),
        })
        .input(CallCommandMessage {
            service_name: "MySvc".to_string(),
            handler_name: "MyHandler".to_string(),
            invocation_id_notification_idx: 3,
            result_completion_id: 4,
            ..CallCommandMessage::default()
        })
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 3,
            invocation_id: "my-id-2".to_string(),
        })
        .input(cancel_signal_notification())
        .input(SendSignalCommandMessage {
            target_invocation_id: "my-id-1".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let call_handle_1 = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            let call_handle_2 = vm
                .sys_call(
                    Target {
                        service: "MySvc".to_string(),
                        handler: "MyHandler".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::new(),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            // First time, responds with any completed, then suspends because it's missing the invocation id to complete the cancellation
            assert_eq!(
                vm.do_await(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(call_handle_1.call_notification_handle),
                    UnresolvedFuture::Single(call_handle_2.call_notification_handle)
                ]))
                .unwrap(),
                AwaitResponse::CancelSignalReceived
            );

            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "my-id-2".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

// Reproduces the is_none_or in is_handle_completed bug:
// a handler that calls a downstream service, awaits it, then sleeps.
// While sleeping, the invocation is cancelled. The handler catches the cancellation and runs a *compensation*:
// a NEW downstream call (the "revert"/undo). The compensation call must run to completion and should not be canceled again.
#[test]
fn saga_compensation_call_is_not_cancelled() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        // Forward "reserve" call resolves normally, *before* the cancellation:
        //  - its invocation id (needed later to propagate the cancel to it)
        //  - its result
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 1,
            invocation_id: "reserve-invocation-id".to_string(),
        })
        .input(CallCompletionNotificationMessage {
            completion_id: 2,
            result: Some(call_completion_notification_message::Result::Value(
                Bytes::from_static(b"reserved").into(),
            )),
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            // ── Forward step: reserve via a downstream service, and await it.
            let reserve = vm
                .sys_call(
                    Target {
                        service: "Inventory".to_string(),
                        handler: "reserve".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::from_static(b"flight"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(reserve.call_notification_handle))
                    .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert2::assert!(
                let Some(Value::Success(reserved)) = vm
                    .take_notification(reserve.call_notification_handle)
                    .unwrap()
            );
            assert_eq!(reserved, Bytes::from_static(b"reserved"));

            // ── Long sleep — the window in which we get cancelled.
            let sleep = vm
                .sys_sleep(String::default(), Duration::from_secs(60), None)
                .unwrap();

            // The cancellation arrives now, while we're sleeping.
            vm.notify_input(encoder.encode(&cancel_signal_notification()));

            // Awaiting the sleep surfaces the cancellation — this is where the
            // saga would `catch` and start compensating.
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(sleep)).unwrap(),
                AwaitResponse::CancelSignalReceived
            );

            // ── Compensation step: unreserve via the downstream service.
            // A NEW call, made AFTER the cancellation was caught. It must run to
            // completion and MUST NOT be cancelled by the runtime.
            let unreserve = vm
                .sys_call(
                    Target {
                        service: "Inventory".to_string(),
                        handler: "unreserve".to_string(),
                        key: None,
                        idempotency_key: None,
                        scope: None,
                        limit_key: None,
                        headers: Vec::new(),
                    },
                    Bytes::from_static(b"flight"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();

            // Before the fix, this returned CancelSignalReceived.
            // With the fix, it completes normally.
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(unreserve.call_notification_handle))
                    .unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            // Send notifications for unreserve
            vm.notify_input(
                encoder.encode(&CallInvocationIdCompletionNotificationMessage {
                    completion_id: 4,
                    invocation_id: "unreserve-invocation-id".to_string(),
                }),
            );
            vm.notify_input(encoder.encode(&CallCompletionNotificationMessage {
                completion_id: 5,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"unreserved").into(),
                )),
            }));
            vm.notify_input_closed();

            // Before the fix, this returned CancelSignalReceived.
            // With the fix, it completes normally.
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(unreserve.call_notification_handle))
                    .unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert2::assert!(
                let Some(Value::Success(unreserved)) = vm
                    .take_notification(unreserve.call_notification_handle)
                    .unwrap()
            );
            assert_eq!(unreserved, Bytes::from_static(b"unreserved"));

            vm.sys_write_output(
                NonEmptyValue::Success(unreserved),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

    // Forward reserve call.
    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("Inventory"),
            handler_name: eq("reserve"),
            invocation_id_notification_idx: eq(1),
            result_completion_id: eq(2),
        })
    );
    // The sleep.
    assert_that!(
        output.next_decoded::<SleepCommandMessage>().unwrap(),
        pat!(SleepCommandMessage {
            result_completion_id: eq(3)
        })
    );
    // Cancellation propagates to the in-flight forward call.
    assert_eq!(
        output.next_decoded::<SendSignalCommandMessage>().unwrap(),
        SendSignalCommandMessage {
            target_invocation_id: "reserve-invocation-id".to_string(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
            ..Default::default()
        }
    );
    // The compensation call is journaled...
    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("Inventory"),
            handler_name: eq("unreserve"),
            invocation_id_notification_idx: eq(4),
            result_completion_id: eq(5),
        })
    );
    // The awaiting on the new call
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(Future {
                waiting_signals: unordered_elements_are![eq(1)],
                waiting_completions: unordered_elements_are![eq(5)],
                waiting_named_signals: empty(),
                nested_futures: empty(),
                combinator_type: eq(CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
        })
    );
    // Finally, the output. No SendSignalCommandMessage targeting "unreserve-invocation-id".
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"unreserved")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
