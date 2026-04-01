use super::*;

use crate::service_protocol::messages::{
    signal_notification_message, AwaitingOnMessage, EndMessage, GetLazyStateCommandMessage,
    SignalNotificationMessage, SuspensionMessage,
};
use crate::Value;
use test_log::test;

#[test]
fn trigger_suspension_with_get_state() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            let _ = vm.sys_input().unwrap();

            let handle = vm
                .sys_state_get("Personaggio".to_owned(), PayloadOptions::default())
                .unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_notification(handle), ok(none()));

            // Let's notify_input_closed now
            vm.notify_input_closed();
            assert_that!(
                vm.do_progress(UnresolvedFuture::Single(handle)),
                err(is_suspended())
            );
        });

    // Assert output
    assert_eq!(
        output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
        GetLazyStateCommandMessage {
            key: Bytes::from_static(b"Personaggio"),
            result_completion_id: 1,
            ..Default::default()
        }
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn trigger_suspension_with_correct_awakeable() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let (_, _h1) = vm.sys_awakeable().unwrap();
            let (_, h2) = vm.sys_awakeable().unwrap();

            // Also take_async_result returns Ok(None)
            assert_that!(vm.take_notification(h2), ok(none()));

            // Let's notify_input_closed now
            vm.notify_input_closed();
            assert_that!(
                vm.do_progress(UnresolvedFuture::Single(h2)),
                err(is_suspended())
            );
        });

    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: empty(),
                waiting_signals: unordered_elements_are![eq(1), eq(18)],
                nested_futures: empty(),
                waiting_named_signals: empty(),
                combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
            }))
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn await_many_notifications() {
    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();

            let (_, h1) = vm.sys_awakeable().unwrap();
            let h2 = vm.create_signal_handle("abc".into()).unwrap();
            let h3 = vm
                .sys_state_get("Personaggio".to_owned(), PayloadOptions::default())
                .unwrap();

            // Let's notify_input_closed now
            vm.notify_input_closed();
            assert_that!(
                vm.do_progress(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(h1),
                    UnresolvedFuture::Single(h2),
                    UnresolvedFuture::Single(h3)
                ])),
                err(is_suspended())
            );
        });

    assert_eq!(
        output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
        GetLazyStateCommandMessage {
            key: Bytes::from_static(b"Personaggio"),
            result_completion_id: 1,
            ..Default::default()
        }
    );
    // Cancel signal wraps the original future: FirstCompleted([original, cancel])
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: empty(),
                waiting_signals: eq(&[1]),
                waiting_named_signals: empty(),
                nested_futures: elements_are![pat!(messages::Future {
                    waiting_completions: eq(&[1]),
                    waiting_signals: eq(&[17]),
                    waiting_named_signals: eq(&["abc".to_owned()]),
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
fn when_notify_completion_then_notify_await_point_then_notify_input_closed_then_no_suspension() {
    let completion = Bytes::from_static(b"completion");

    let mut output = VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let (_, h1) = vm.sys_awakeable().unwrap();
            let (_, h2) = vm.sys_awakeable().unwrap();

            // Do progress will ask for more input
            assert_that!(
                vm.do_progress(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(h1),
                    UnresolvedFuture::Single(h2)
                ])),
                ok(eq(DoProgressResponse::ReadFromInput))
            );

            // Let's send Completion for h2
            vm.notify_input(encoder.encode(&SignalNotificationMessage {
                signal_id: Some(signal_notification_message::SignalId::Idx(18)),
                result: Some(signal_notification_message::Result::Value(
                    completion.clone().into(),
                )),
            }));

            // This should not suspend
            vm.notify_input_closed();
            assert_that!(
                vm.do_progress(UnresolvedFuture::FirstCompleted(vec![
                    UnresolvedFuture::Single(h1),
                    UnresolvedFuture::Single(h2)
                ])),
                ok(eq(DoProgressResponse::AnyCompleted))
            );

            // H2 should be completed and we can take it
            assert!(vm.is_completed(h2));
            assert_that!(
                vm.take_notification(h2),
                ok(some(eq(Value::Success(completion.clone()))))
            );

            vm.sys_write_output(
                NonEmptyValue::Success(completion.clone()),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

    // First do_progress returned ReadFromInput, emits AwaitingOnMessage
    // Future is FirstCompleted([FirstCompleted([Single(signal_17), Single(signal_18)]), Single(cancel)])
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_signals: eq(vec![1]),
                nested_futures: elements_are![pat!(messages::Future {
                    waiting_signals: unordered_elements_are![eq(17), eq(18)],
                    combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
                })],
                combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
        })
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(completion)
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
