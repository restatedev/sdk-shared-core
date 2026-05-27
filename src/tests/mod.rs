mod async_result;
mod calls;
mod derive_roundtrip;
mod failures;
mod host_payload;
mod implicit_cancellation;
mod input_output;
mod promise;
mod run;
mod sleep;
mod state;
mod suspensions;

use super::*;

use crate::buffer::BufferWriter;
use crate::service_protocol::messages::{
    output_command_message, signal_notification_message, ErrorMessage, InputCommandMessage,
    OutputCommandMessage, RestateEncodableMessage, RestateMessage, SignalNotificationMessage,
    StartMessage, SuspensionMessage,
};
use crate::service_protocol::{
    messages, CompletionId, Decoder, MessageHeader, RawMessage, Version,
};
use crate::Buffer;
use bytes::{BufMut, Bytes, BytesMut};
use googletest::prelude::*;
use test_log::test;

#[macro_export]
macro_rules! first_completed {
    ($($child:expr),+ $(,)?) => {
        UnresolvedFuture::FirstCompleted(vec![$(Into::<UnresolvedFuture>::into($child)),+])
    };
}
#[macro_export]
macro_rules! all_completed {
    ($($child:expr),+ $(,)?) => {
        UnresolvedFuture::AllCompleted(vec![$(Into::<UnresolvedFuture>::into($child)),+])
    };
}
#[macro_export]
macro_rules! first_succeeded_or_all_failed {
    ($($child:expr),+ $(,)?) => {
        UnresolvedFuture::FirstSucceededOrAllFailed(vec![$(Into::<UnresolvedFuture>::into($child)),+])
    };
}
#[macro_export]
macro_rules! all_succeeded_or_first_failed {
    ($($child:expr),+ $(,)?) => {
        UnresolvedFuture::AllSucceededOrFirstFailed(vec![$(Into::<UnresolvedFuture>::into($child)),+])
    };
}
#[macro_export]
macro_rules! unknown {
    ($($child:expr),+ $(,)?) => {
        UnresolvedFuture::Unknown(vec![$(Into::<UnresolvedFuture>::into($child)),+])
    };
}

impl From<u32> for UnresolvedFuture {
    fn from(value: u32) -> Self {
        NotificationHandle::from(value).into()
    }
}

// --- Test infra

/// Tests-only message encoder. Wraps `BufferWriter` and flattens its
/// output into a single `Bytes` so tests can feed a contiguous wire
/// buffer into `Decoder::push` or `CoreVM::notify_input`. Prod doesn't
/// need this — `Output::send` writes directly into the long-lived
/// writer and the embedder drains buffers one at a time.
pub struct Encoder {
    service_protocol_version: Version,
}

impl Encoder {
    pub fn new(service_protocol_version: Version) -> Self {
        assert!(
            service_protocol_version >= Version::minimum_supported_version(),
            "Encoder only supports service protocol version {:?} <= x <= {:?}",
            Version::minimum_supported_version(),
            Version::maximum_supported_version()
        );
        Self {
            service_protocol_version,
        }
    }

    /// Encode a protocol message into a single contiguous `Bytes`.
    /// Panics on `Buffer::Host` payloads — by design, tests that need
    /// host payloads should go through `Output::send` instead.
    pub fn encode<M: RestateEncodableMessage>(&self, msg: &M) -> Bytes {
        let mut buf = BufferWriter::new();
        msg.encode_into(self.service_protocol_version, &mut buf);
        let mut out = BytesMut::new();
        for buffer in buf.drain() {
            match buffer {
                Buffer::InMemory(b) => out.put_slice(&b),
                Buffer::Host(_) => panic!(
                    "Encoder::encode hit a Buffer::Host — host buffers must \
                     flow through Output::send instead."
                ),
            }
        }
        out.freeze()
    }
}

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
    encoder: Encoder,
    vm: CoreVM,
}

impl VMTestCase {
    fn new() -> Self {
        Self {
            encoder: Encoder::new(Version::maximum_supported_version()),
            vm: CoreVM::mock_init(Version::maximum_supported_version()),
        }
    }

    fn with_vm_options(options: VMOptions) -> Self {
        Self {
            encoder: Encoder::new(Version::maximum_supported_version()),
            vm: CoreVM::mock_init_with_options(Version::maximum_supported_version(), options),
        }
    }

    fn with_version(version: Version) -> Self {
        Self {
            encoder: Encoder::new(version),
            vm: CoreVM::mock_init_with_options(version, Default::default()),
        }
    }

    fn input<M: RestateEncodableMessage>(mut self, m: M) -> Self {
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

struct OutputIterator {
    decoder: Decoder,
}

/// Test helper: drain `vm.take_output_next` and concatenate every inline
/// fragment into a single `Bytes`. Panics on `Host` fragments — all
/// existing tests use inline payloads only.
fn drain_and_concat(vm: &mut impl VM) -> Bytes {
    let mut buf = bytes::BytesMut::new();
    while let Some(fragment) = vm.take_output_next() {
        match fragment {
            Buffer::InMemory(b) => buf.extend_from_slice(&b),
            Buffer::Host(_) => {
                panic!("drain_and_concat: Host fragments not supported in this test helper")
            }
        }
    }
    buf.freeze()
}

impl OutputIterator {
    fn collect_vm(vm: &mut impl VM) -> Self {
        let mut decoder = Decoder::new(Version::maximum_supported_version());
        decoder.push(drain_and_concat(vm));
        Self { decoder }
    }

    fn next_decoded<M: RestateMessage>(&mut self) -> Option<M> {
        self.decoder
            .consume_next()
            .unwrap()
            .map(|msg| msg.decode_to::<M>(0).unwrap())
    }

    fn next_with_header_decoded<M: RestateMessage>(&mut self) -> Option<(MessageHeader, M)> {
        self.decoder.consume_next().unwrap().map(|raw| {
            let header = raw.header();
            let msg = raw.decode_to::<M>(0).unwrap();
            (header, msg)
        })
    }
}

impl Iterator for OutputIterator {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.consume_next().unwrap()
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
        awaiting_on: some(pat!(messages::Future {
            waiting_completions: eq(vec![completion_id]),
            waiting_signals: eq(vec![1]),
            nested_futures: empty(),
            waiting_named_signals: empty(),
            combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
        }))
    })
}

pub fn suspended_waiting_signal(signal_idx: u32) -> impl Matcher<ActualT = SuspensionMessage> {
    pat!(SuspensionMessage {
        awaiting_on: some(pat!(messages::Future {
            waiting_completions: empty(),
            waiting_signals: all!(contains(eq(signal_idx)), contains(eq(1))),
            nested_futures: empty(),
            waiting_named_signals: empty(),
            combinator_type: eq(messages::CombinatorType::FirstCompleted as i32)
        }))
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
        scope: None,
        limit_key: None,
        idempotency_key: None,
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

pub fn empty_signal_notification(id: u32) -> SignalNotificationMessage {
    SignalNotificationMessage {
        signal_id: Some(signal_notification_message::SignalId::Idx(id)),
        result: Some(signal_notification_message::Result::Void(Default::default())),
    }
}

#[test]
fn take_output_on_newly_initialized_vm() {
    let mut vm = CoreVM::mock_init(Version::maximum_supported_version());
    // A fresh VM has no output queued yet.
    assert!(vm.take_output_next().is_none());
}

#[test]
fn instantiate_core_vm_minimum_supported_version() {
    CoreVM::mock_init(Version::minimum_supported_version());
}
