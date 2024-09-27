mod async_result;
mod failures;
mod get_state;
mod input_output;
mod promise;
mod run;
mod sleep;
mod state;
mod suspensions;

use super::*;

use crate::service_protocol::messages::{
    output_entry_message, run_entry_message, ErrorMessage, InputEntryMessage, OutputEntryMessage,
    RestateMessage, RunEntryMessage, StartMessage, SuspensionMessage, WriteableRestateMessage,
};
use crate::service_protocol::{messages, Decoder, Encoder, RawMessage, Version};
use bytes::Bytes;
use googletest::prelude::*;
use std::result::Result;
use test_log::test;

// --- Test infra

impl CoreVM {
    fn mock_init(version: Version) -> CoreVM {
        let vm = CoreVM::new(
            vec![("content-type".to_owned(), version.to_string())],
            VMOptions::default(),
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
    encoder: Encoder,
    vm: CoreVM,
}

impl VMTestCase {
    fn new() -> Self {
        Self {
            encoder: Encoder::new(Version::latest()),
            vm: CoreVM::mock_init(Version::latest()),
        }
    }

    fn input<M: WriteableRestateMessage>(mut self, m: M) -> Self {
        self.vm.notify_input(self.encoder.encode(&m));
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
        let mut decoder = Decoder::new(Version::latest());
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
            .map(|msg| msg.decode_to::<M>().unwrap())
    }
}

impl Iterator for OutputIterator {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.consume_next().unwrap()
    }
}

// --- Matchers

/// Matcher for VMError
pub fn eq_vm_error(vm_error: VMError) -> impl Matcher<ActualT = VMError> {
    pat!(VMError {
        code: eq(vm_error.code),
        message: eq(vm_error.message),
        description: eq(vm_error.description)
    })
}

/// Matcher for ErrorMessage to equal VMError
pub fn error_message_as_vm_error(vm_error: VMError) -> impl Matcher<ActualT = ErrorMessage> {
    pat!(ErrorMessage {
        code: eq(vm_error.code as u32),
        message: eq(vm_error.message),
        description: eq(vm_error.description)
    })
}

pub fn suspended_with_index(index: u32) -> impl Matcher<ActualT = SuspensionMessage> {
    pat!(SuspensionMessage {
        entry_indexes: eq(vec![index])
    })
}

pub fn is_suspended() -> impl Matcher<ActualT = SuspendedOrVMError> {
    pat!(SuspendedOrVMError::Suspended(_))
}

#[allow(dead_code)]
pub fn is_run_with_success(b: impl AsRef<[u8]>) -> impl Matcher<ActualT = RunEntryMessage> {
    pat!(RunEntryMessage {
        result: some(pat!(run_entry_message::Result::Value(eq(
            Bytes::copy_from_slice(b.as_ref())
        ))))
    })
}

pub fn is_run_with_failure(
    code: u16,
    message: impl Into<String>,
) -> impl Matcher<ActualT = RunEntryMessage> {
    pat!(RunEntryMessage {
        result: some(pat!(run_entry_message::Result::Failure(eq(
            messages::Failure {
                code: code as u32,
                message: message.into(),
            }
        ))))
    })
}

pub fn is_output_with_success(b: impl AsRef<[u8]>) -> impl Matcher<ActualT = OutputEntryMessage> {
    pat!(OutputEntryMessage {
        result: some(pat!(output_entry_message::Result::Value(eq(
            Bytes::copy_from_slice(b.as_ref())
        ))))
    })
}

pub fn is_output_with_failure(
    code: u16,
    message: impl Into<String>,
) -> impl Matcher<ActualT = OutputEntryMessage> {
    pat!(OutputEntryMessage {
        result: some(pat!(output_entry_message::Result::Failure(eq(
            messages::Failure {
                code: code as u32,
                message: message.into(),
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
    }
}

pub fn input_entry_message(b: impl AsRef<[u8]>) -> InputEntryMessage {
    InputEntryMessage {
        headers: vec![],
        value: Bytes::copy_from_slice(b.as_ref()),
        ..InputEntryMessage::default()
    }
}

#[test]
fn take_output_on_newly_initialized_vm() {
    let mut vm = CoreVM::mock_init(Version::latest());
    assert_that!(
        vm.take_output(),
        eq(TakeOutputResult::Buffer(Bytes::default()))
    );
}
