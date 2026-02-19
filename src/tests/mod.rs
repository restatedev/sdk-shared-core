mod async_result;
mod calls;
mod failures;
mod implicit_cancellation;
mod input_output;
mod promise;
mod run;
mod sleep;
mod state;
mod suspensions;

use super::*;

use crate::service_protocol::messages::{
    output_command_message, signal_notification_message, EagerStateCompleteMessage,
    EagerStateEntryMessage, ErrorMessage, InputCommandMessage, OutputCommandMessage,
    RestateMessage, SignalNotificationMessage, StartMessage, SuspensionMessage,
};
use crate::service_protocol::{messages, CompletionId, Decoder, Encoder, RawMessage, Version};
use bytes::Bytes;
use googletest::prelude::*;
use test_log::test;

// --- Test infra

impl CoreVM {
    fn mock_init(version: Version) -> CoreVM {
        Self::mock_init_with_options(version, Default::default())
    }

    fn mock_init_with_options(version: Version, options: VMOptions) -> CoreVM {
        let vm = CoreVM::new(
            vec![("content-type".to_owned(), version.to_string())],
            options,
        )
        .unwrap();

        assert_that!(
            vm.get_response_head().headers,
            contains(eq(Header {
                key: Cow::Borrowed("content-type"),
                value: Cow::Borrowed(version.content_type())
            }))
        );

        vm
    }
}

struct VMTestCase {
    version: Version,
    encoder: Encoder,
    vm: CoreVM,
}

impl VMTestCase {
    fn new() -> Self {
        Self::with_version(Version::maximum_supported_version())
    }

    fn with_version(version: Version) -> Self {
        Self {
            version,
            encoder: Encoder::new(version),
            vm: CoreVM::mock_init(version),
        }
    }

    fn with_vm_options(options: VMOptions) -> Self {
        Self {
            version: Version::maximum_supported_version(),
            encoder: Encoder::new(Version::maximum_supported_version()),
            vm: CoreVM::mock_init_with_options(Version::maximum_supported_version(), options),
        }
    }

    fn input<M: RestateMessage>(mut self, m: M) -> Self {
        self.vm.notify_input(self.encoder.encode(&m));
        self
    }

    /// Send a StartMessage with V7 eager state streaming support.
    /// In V7+, state_map entries are extracted from the StartMessage and sent
    /// as EagerStateEntry/EagerStateComplete messages after it.
    /// In V5/V6, the StartMessage is sent as-is.
    fn input_start(mut self, msg: StartMessage) -> Self {
        if self.version >= Version::V7 {
            let state_map = msg.state_map.clone();
            let partial_state = msg.partial_state;

            // Send StartMessage with empty state_map for V7
            let v7_start = StartMessage {
                state_map: vec![],
                partial_state: false, // irrelevant in V7
                ..msg
            };
            self.vm.notify_input(self.encoder.encode(&v7_start));

            // Stream state entries if any
            if !state_map.is_empty() {
                self.vm
                    .notify_input(self.encoder.encode(&EagerStateEntryMessage { state_map }));
            }

            // Send the terminator
            self.vm.notify_input(
                self.encoder
                    .encode(&EagerStateCompleteMessage { partial_state }),
            );
        } else {
            self.vm.notify_input(self.encoder.encode(&msg));
        }
        self
    }

    fn run(mut self, user_code: impl FnOnce(&mut CoreVM)) -> OutputIterator {
        self.vm.notify_input_closed();
        assert!(self.vm.is_ready_to_execute().unwrap());

        user_code(&mut self.vm);

        OutputIterator::collect_vm(&mut self.vm)
    }

    fn run_without_closing_input(
        mut self,
        user_code: impl FnOnce(&mut CoreVM, &Encoder),
    ) -> OutputIterator {
        assert!(self.vm.is_ready_to_execute().unwrap());

        user_code(&mut self.vm, &self.encoder);

        OutputIterator::collect_vm(&mut self.vm)
    }
}

struct OutputIterator(Decoder);

impl OutputIterator {
    fn collect_vm(vm: &mut impl VM) -> Self {
        let mut decoder = Decoder::new(Version::maximum_supported_version());
        while let TakeOutputResult::Buffer(b) = vm.take_output() {
            decoder.push(b);
        }
        assert_eq!(vm.take_output(), TakeOutputResult::EOF);

        Self(decoder)
    }

    fn next_decoded<M: RestateMessage>(&mut self) -> Option<M> {
        self.0
            .consume_next()
            .unwrap()
            .map(|msg| msg.decode_to::<M>(0).unwrap())
    }
}

impl Iterator for OutputIterator {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.consume_next().unwrap()
    }
}

// --- Matchers

/// Matcher for Error
pub fn eq_error(vm_error: Error) -> impl Matcher<ActualT = Error> {
    pat!(Error {
        code: eq(vm_error.code),
        message: eq(vm_error.message),
        stacktrace: eq(vm_error.stacktrace)
    })
}

/// Matcher for ErrorMessage to equal Error
pub fn error_message_as_error(vm_error: Error) -> impl Matcher<ActualT = ErrorMessage> {
    pat!(ErrorMessage {
        code: eq(vm_error.code as u32),
        message: eq(vm_error.message),
        stacktrace: eq(vm_error.stacktrace)
    })
}

pub fn suspended_waiting_completion(
    completion_id: CompletionId,
) -> impl Matcher<ActualT = SuspensionMessage> {
    pat!(SuspensionMessage {
        waiting_completions: eq(vec![completion_id]),
        waiting_signals: eq(vec![1])
    })
}

pub fn suspended_waiting_signal(signal_idx: u32) -> impl Matcher<ActualT = SuspensionMessage> {
    pat!(SuspensionMessage {
        waiting_signals: all!(contains(eq(signal_idx)), contains(eq(1)))
    })
}

pub fn is_suspended() -> impl Matcher<ActualT = Error> {
    predicate(|e: &Error| e.is_suspended_error())
        .with_description("is suspended error", "is not suspended error")
}

pub fn is_closed() -> impl Matcher<ActualT = Error> {
    predicate(|e: &Error| e.code == error::codes::CLOSED.code())
        .with_description("is closed error", "is not closed error")
}

pub fn is_output_with_success(b: impl AsRef<[u8]>) -> impl Matcher<ActualT = OutputCommandMessage> {
    pat!(OutputCommandMessage {
        result: some(pat!(output_command_message::Result::Value(eq(
            Bytes::copy_from_slice(b.as_ref()).into()
        ))))
    })
}

pub fn is_output_with_failure(
    code: u16,
    message: impl Into<String>,
) -> impl Matcher<ActualT = OutputCommandMessage> {
    pat!(OutputCommandMessage {
        result: some(pat!(output_command_message::Result::Failure(eq(
            messages::Failure {
                code: code as u32,
                message: message.into(),
                metadata: vec![],
            }
        ))))
    })
}

// --- Mocks

pub fn start_message(known_entries: u32) -> StartMessage {
    StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries,
        state_map: vec![],
        partial_state: true,
        key: "".to_string(),
        retry_count_since_last_stored_entry: 0,
        duration_since_last_stored_entry: 0,
        random_seed: 0,
    }
}

pub fn input_entry_message(b: impl AsRef<[u8]>) -> InputCommandMessage {
    InputCommandMessage {
        headers: vec![],
        value: Some(Bytes::copy_from_slice(b.as_ref()).into()),
        ..InputCommandMessage::default()
    }
}

pub fn cancel_signal_notification() -> SignalNotificationMessage {
    SignalNotificationMessage {
        signal_id: Some(signal_notification_message::SignalId::Idx(1)),
        result: Some(signal_notification_message::Result::Void(Default::default())),
    }
}

#[test]
fn take_output_on_newly_initialized_vm() {
    let mut vm = CoreVM::mock_init(Version::maximum_supported_version());
    assert_that!(
        vm.take_output(),
        eq(TakeOutputResult::Buffer(Bytes::default()))
    );
}

#[test]
fn instantiate_core_vm_minimum_supported_version() {
    CoreVM::mock_init(Version::minimum_supported_version());
}
