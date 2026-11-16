//! How an attempt ends when the runtime closes the input stream, or when it fails.

use super::*;
use test_log::test;

use crate::service_protocol::messages::{
    CallCommandMessage, CallInvocationIdCompletionNotificationMessage, CombinatorType, Future,
    GetLazyStateCommandMessage, GetStateEphemeralCommandMessage, OneWayCallCommandMessage,
    SuspensionMessage,
};
use crate::Error;

fn running() -> ErTestCase {
    ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
}

fn suspended_on(waiting_completions: Vec<u32>, waiting_signals: Vec<u32>) -> SuspensionMessage {
    SuspensionMessage {
        awaiting_on: Some(Future {
            waiting_completions,
            waiting_signals,
            combinator_type: CombinatorType::FirstCompleted as i32,
            ..Default::default()
        }),
    }
}

/// After the shutdown, every call fails and nothing more is written.
fn assert_shut_down(vm: &mut CoreExplicitRecoveryVM) {
    assert_eq!(vm.state(), AttemptState::Closed);
    assert_that!(
        vm.take_next_notification(),
        err(predicate(Error::is_suspended_error))
    );
    assert_that!(
        vm.cmd_state_set(
            "my-key".to_owned(),
            Bytes::from_static(b"my-value"),
            None,
            false
        ),
        err(predicate(Error::is_suspended_error))
    );
    assert_that!(
        vm.end(NonEmptyValue::Success(Bytes::new())),
        err(predicate(Error::is_suspended_error))
    );
    assert_that!(vm.heartbeat(), err(predicate(Error::is_suspended_error)));
    vm.notify_error(Error::internal("too late"));
}

fn assert_restarted(output: &mut OutputIterator) {
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(u32::from(codes::INTERNAL)),
            next_retry_delay: some(eq(0)),
            behavior: eq(ErrorBehavior::Retry as i32)
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn shutdown_suspends_on_the_pending_calls() {
    let mut output = running().run(|vm, encoder| {
        vm.get_input().unwrap();
        vm.cmd_call(target(), Bytes::new(), Default::default())
            .unwrap();
        vm.cmd_call(target(), Bytes::new(), Default::default())
            .unwrap();
        notify(
            vm,
            encoder,
            CallInvocationIdCompletionNotificationMessage {
                completion_id: 1,
                invocation_id: "my-invocation".to_owned(),
            },
        );
        vm.notify_input_closed();
        assert_shut_down(vm);
    });

    output.next_decoded::<CallCommandMessage>().unwrap();
    output.next_decoded::<CallCommandMessage>().unwrap();
    // The results only: an invocation id is stored with its call
    assert_eq!(
        {
            let mut suspension = output.next_decoded::<SuspensionMessage>().unwrap();
            sort_completions(&mut suspension.awaiting_on);
            suspension
        },
        suspended_on(vec![2, 3, 4], vec![])
    );
    assert_eq!(output.next(), None);
}

#[test]
fn shutdown_suspends_on_every_pending_completion() {
    let mut output = ErTestCase::with_version(Version::V7)
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            vm.get_input().unwrap();
            vm.cmd_state_get("my-key".to_owned(), None).unwrap();
            vm.cmd_send(target(), Bytes::new(), None, Default::default())
                .unwrap();
            vm.cmd_state_set("my-key".to_owned(), Bytes::new(), None, true)
                .unwrap();
            vm.notify_input_closed();
            assert_shut_down(vm);
        });

    output.next_decoded::<GetLazyStateCommandMessage>().unwrap();
    output.next_decoded::<OneWayCallCommandMessage>().unwrap();
    output.next_decoded::<SetStateCommandMessage>().unwrap();
    // The lazy get and the send's invocation id, not the ack, which can't be waited on
    assert_eq!(
        {
            let mut suspension = output.next_decoded::<SuspensionMessage>().unwrap();
            sort_completions(&mut suspension.awaiting_on);
            suspension
        },
        suspended_on(vec![1, 2], vec![])
    );
    assert_eq!(output.next(), None);
}

#[test]
fn shutdown_with_an_ephemeral_get_pending_restarts() {
    let mut output = running().run(|vm, _| {
        vm.get_input().unwrap();
        vm.cmd_state_get("my-key".to_owned(), None).unwrap();
        vm.notify_input_closed();
        assert_shut_down(vm);
    });

    output
        .next_decoded::<GetStateEphemeralCommandMessage>()
        .unwrap();
    assert_restarted(&mut output);
}

#[test]
fn shutdown_with_nothing_pending_restarts() {
    let mut output = running().run(|vm, _| {
        vm.get_input().unwrap();
        vm.notify_input_closed();
        assert_shut_down(vm);
    });

    assert_restarted(&mut output);
}

#[test]
fn notify_error_fails_the_attempt() {
    let mut output = running().run(|vm, _| {
        vm.get_input().unwrap();
        vm.cmd_call(target(), Bytes::new(), Default::default())
            .unwrap();
        vm.notify_error(Error::new(503u16, "my-error"));

        assert_eq!(vm.state(), AttemptState::Closed);
        assert_that!(
            vm.take_next_notification(),
            err(eq_error(Error::new(503u16, "my-error")))
        );
        assert_that!(
            vm.cmd_state_get("my-key".to_owned(), None),
            err(eq_error(Error::new(503u16, "my-error")))
        );
        // Closing the input afterwards changes nothing
        vm.notify_input_closed();
    });

    output.next_decoded::<CallCommandMessage>().unwrap();
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(Error::new(503u16, "my-error"))
    );
    assert_eq!(output.next(), None);
}
