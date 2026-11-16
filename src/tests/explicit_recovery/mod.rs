//! Tests of [`crate::explicit_recovery`].

mod actions;
mod earlier_attempts;
mod shutdown;

use super::*;
use test_log::test;

use crate::explicit_recovery::{
    request_response_unsupported, AttemptState, CoreExplicitRecoveryVM, ExplicitRecoveryVM,
    Notification,
};
use crate::service_protocol::messages::{
    EndMessage, ErrorBehavior, ErrorMessage, InputCommandMessage, OutputCommandMessage,
    SetStateCommandMessage, StartMessage,
};
use crate::vm::errors::codes;
use crate::{Header, NonEmptyValue, Target, Value};
use std::borrow::Cow;

// --- Test infra

struct ErTestCase {
    encoder: Encoder,
    vm: CoreExplicitRecoveryVM,
}

impl ErTestCase {
    fn new() -> Self {
        Self::with_version(Version::V8)
    }

    fn with_version(version: Version) -> Self {
        Self {
            encoder: Encoder::new(version),
            vm: CoreExplicitRecoveryVM::new(vec![("content-type".to_owned(), version.to_string())])
                .unwrap(),
        }
    }

    fn input<M: RestateEncodableMessage>(mut self, m: M) -> Self {
        self.vm.notify_input(self.encoder.encode(&m));
        self
    }

    /// Runs the handler. The input stream stays open: closing it shuts the attempt down.
    fn run(
        mut self,
        handler: impl FnOnce(&mut CoreExplicitRecoveryVM, &Encoder),
    ) -> OutputIterator {
        assert!(self.vm.is_ready_to_execute().unwrap());
        handler(&mut self.vm, &self.encoder);
        OutputIterator::from_take_output(|| self.vm.take_output())
    }
}

fn notify<M: RestateEncodableMessage>(vm: &mut CoreExplicitRecoveryVM, encoder: &Encoder, m: M) {
    vm.notify_input(encoder.encode(&m));
}

/// The notifications taken, compared regardless of their order: the VM gives them in no order.
#[derive(Debug)]
struct Taken(Vec<Notification>);

impl PartialEq<Vec<Notification>> for Taken {
    fn eq(&self, other: &Vec<Notification>) -> bool {
        self.0.len() == other.len()
            && self.0.iter().all(|n| other.contains(n))
            && other.iter().all(|n| self.0.contains(n))
    }
}

fn take_notifications(vm: &mut CoreExplicitRecoveryVM) -> Taken {
    let mut notifications = vec![];
    while let Some(notification) = vm.take_next_notification().unwrap() {
        notifications.push(notification);
    }
    Taken(notifications)
}

/// The completion ids of `future` sorted, to compare them regardless of order: the VM lists them
/// in no order.
fn sort_completions(future: &mut Option<messages::Future>) {
    if let Some(future) = future {
        future.waiting_completions.sort();
    }
}

fn notification(handle: NotificationHandle, value: Value) -> Notification {
    Notification { handle, value }
}

fn success(value: &'static str) -> Value {
    Value::Success(Bytes::from_static(value.as_bytes()))
}

fn target() -> Target {
    Target {
        service: "MySvc".to_owned(),
        handler: "MyHandler".to_owned(),
        key: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        headers: vec![],
    }
}

fn end_with(vm: &mut CoreExplicitRecoveryVM, output: &'static str) {
    vm.end(NonEmptyValue::Success(Bytes::from_static(
        output.as_bytes(),
    )))
    .unwrap();
}

fn assert_end(output: &mut OutputIterator, value: &'static str) {
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(value)
    );
    assert_eq!(output.next_decoded::<EndMessage>().unwrap(), EndMessage {});
    assert_eq!(output.next(), None);
}

// --- Start

#[test]
fn protocols_before_v7_are_refused() {
    for version in [Version::V5, Version::V6] {
        let error =
            CoreExplicitRecoveryVM::new(vec![("content-type".to_owned(), version.to_string())])
                .unwrap_err();
        assert_eq!(error.code(), codes::UNSUPPORTED_MEDIA_TYPE.code());
    }
}

#[test]
fn response_head_has_the_negotiated_version() {
    for version in [Version::V7, Version::V8] {
        let vm = ErTestCase::with_version(version).vm;
        assert_that!(
            vm.get_response_head().headers,
            contains(eq(Header {
                key: Cow::Borrowed("content-type"),
                value: Cow::Borrowed(version.content_type())
            }))
        );
    }
}

#[test]
fn input_returns_the_request() {
    let mut output = ErTestCase::new()
        .input(StartMessage {
            key: "my-key".to_owned(),
            scope: Some("my-scope".to_owned()),
            limit_key: Some("my-limit-key".to_owned()),
            idempotency_key: Some("my-idempotency-key".to_owned()),
            random_seed: 42,
            ..start_message(1)
        })
        .input(InputCommandMessage {
            headers: vec![messages::Header {
                key: "x-my-header".to_owned(),
                value: "my-value".to_owned(),
            }],
            value: Some(Bytes::from_static(b"my-input").into()),
            ..Default::default()
        })
        .run(|vm, _| {
            assert_eq!(vm.state(), AttemptState::Running);
            assert_eq!(
                vm.get_input().unwrap(),
                Input {
                    invocation_id: "123".to_owned(),
                    random_seed: 42,
                    key: "my-key".to_owned(),
                    headers: vec![Header {
                        key: Cow::Owned("x-my-header".to_owned()),
                        value: Cow::Owned("my-value".to_owned()),
                    }],
                    input: Bytes::from_static(b"my-input"),
                    scope: Some("my-scope".to_owned()),
                    limit_key: Some("my-limit-key".to_owned()),
                    idempotency_key: Some("my-idempotency-key".to_owned()),
                }
            );
            assert_eq!(vm.state(), AttemptState::Running);
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn not_ready_before_the_earlier_entries() {
    let mut test_case = ErTestCase::new().input(start_message(2));
    assert!(!test_case.vm.is_ready_to_execute().unwrap());
    test_case = test_case.input(input_entry_message(b"my-input"));
    assert!(!test_case.vm.is_ready_to_execute().unwrap());
    assert_eq!(test_case.vm.state(), AttemptState::Starting);
    test_case = test_case.input(SetStateCommandMessage::default());
    assert!(test_case.vm.is_ready_to_execute().unwrap());
    assert_eq!(test_case.vm.state(), AttemptState::Running);
}

#[test]
fn end_closes_the_attempt() {
    let mut output = ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            vm.get_input().unwrap();
            end_with(vm, "my-output");

            assert_eq!(vm.state(), AttemptState::Closed);
            assert_eq!(vm.take_next_notification().unwrap(), None);
            assert_that!(
                vm.cmd_state_set(
                    "my-key".to_owned(),
                    Bytes::from_static(b"my-value"),
                    None,
                    false
                ),
                err(has_error_code(codes::CLOSED))
            );
            // Input after the end is ignored
            vm.notify_input(Bytes::from_static(b"garbage"));
            vm.notify_input_closed();
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn known_entries_zero_fails() {
    let mut test_case = ErTestCase::new().input(start_message(0));
    assert_eq!(test_case.vm.state(), AttemptState::Closed);
    assert_that!(test_case.vm.is_ready_to_execute(), err(anything()));

    let mut output = OutputIterator::from_take_output(|| test_case.vm.take_output());
    assert_eq!(
        output.next_decoded::<ErrorMessage>().unwrap().code,
        u32::from(codes::INTERNAL)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn input_closed_while_waiting_the_earlier_entries_fails() {
    let mut test_case = ErTestCase::new()
        .input(start_message(2))
        .input(input_entry_message(b"my-input"));
    test_case.vm.notify_input_closed();

    let mut output = OutputIterator::from_take_output(|| test_case.vm.take_output());
    assert_eq!(
        output.next_decoded::<ErrorMessage>().unwrap().code,
        u32::from(codes::PROTOCOL_VIOLATION)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn input_command_is_found_anywhere_among_the_earlier_entries() {
    let mut output = ErTestCase::new()
        .input(start_message(2))
        .input(SetStateCommandMessage::default())
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            assert_eq!(
                vm.get_input().unwrap().input,
                Bytes::from_static(b"my-input")
            );
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}

#[test]
fn earlier_entries_without_the_input_command_fail() {
    let mut test_case = ErTestCase::new()
        .input(start_message(1))
        .input(SetStateCommandMessage::default());
    assert_eq!(test_case.vm.state(), AttemptState::Closed);

    let mut output = OutputIterator::from_take_output(|| test_case.vm.take_output());
    assert_eq!(
        output.next_decoded::<ErrorMessage>().unwrap().code,
        u32::from(codes::PROTOCOL_VIOLATION)
    );
    assert_eq!(output.next(), None);
}

fn assert_paused(output: &mut OutputIterator) {
    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(u32::from(codes::UNSUPPORTED_FEATURE)),
            behavior: eq(ErrorBehavior::Pause as i32)
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn an_sdk_refuses_request_response_with_a_pause() {
    let mut output = ErTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            vm.notify_error(request_response_unsupported());
            assert_eq!(vm.state(), AttemptState::Closed);
            assert_that!(
                vm.get_input(),
                err(has_error_code(codes::UNSUPPORTED_FEATURE))
            );
        });

    assert_paused(&mut output);
}

#[test]
fn actions_before_input_are_allowed_and_input_can_be_taken_again() {
    let mut output = ErTestCase::new()
        .input(StartMessage {
            partial_state: false,
            ..start_message(1)
        })
        .input(input_entry_message(b"my-input"))
        .run(|vm, _| {
            assert_eq!(vm.state(), AttemptState::Running);
            let read = vm.cmd_state_get("my-key".to_owned(), None).unwrap();
            assert_eq!(
                take_notifications(vm),
                vec![notification(read, Value::Void)]
            );
            assert_eq!(
                vm.get_input().unwrap().input,
                Bytes::from_static(b"my-input")
            );
            assert_eq!(
                vm.get_input().unwrap().input,
                Bytes::from_static(b"my-input")
            );
            end_with(vm, "my-output");
        });

    assert_end(&mut output, "my-output");
}
