// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::header::UnknownMessageType;
use super::messages::RestateMessage;
use super::*;

use crate::buffer::{BufferReader, RestateBuf};
use crate::proto::Message;
use crate::vm::errors::CommandTypeMismatchError;
use crate::Buffer;
use bytes::{Buf, Bytes};

#[derive(Debug, thiserror::Error)]
pub enum DecodingError {
    #[error("cannot decode protocol message type {0:?}. Reason: {1:?}")]
    DecodeMessage(MessageType, #[source] prost::DecodeError),
    #[error(transparent)]
    UnexpectedMessageType(CommandTypeMismatchError),
    #[error("expected message type {expected:?} to have field {field}")]
    MissingField {
        expected: MessageType,
        field: &'static str,
    },
    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),
}

// --- Input protocol.message decoder

/// A protocol message peeled off the decoder, header + body. The body is
/// carried as [`Buffer`] so a host-backed input flows through decode as a
/// `Buffer::Host` sub-view (zero-copy), or as a `Buffer::InMemory` for
/// inline / cross-segment bodies (materialised path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage(MessageHeader, Buffer);

impl RawMessage {
    pub fn ty(&self) -> MessageType {
        self.0.message_type()
    }

    #[cfg(test)]
    pub fn header(&self) -> MessageHeader {
        self.0
    }

    pub fn decode_to<M: RestateMessage>(self, command_index: i64) -> Result<M, DecodingError> {
        if self.0.message_type() != M::ty() {
            return Err(DecodingError::UnexpectedMessageType(
                CommandTypeMismatchError::new(command_index, self.0.message_type(), M::ty()),
            ));
        }
        let ty = self.0.message_type();
        match self.1 {
            Buffer::InMemory(b) => M::decode(b).map_err(|e| DecodingError::DecodeMessage(ty, e)),
            Buffer::Host(h) => {
                // Wrap the single-segment host body in a BufferReader so
                // prost decodes via the same Buf + RestateBuf machinery
                // used by the protocol Decoder. Single-segment means
                // `try_take_host_slice` succeeds whenever a `bytes =
                // "buffer"` field fits in the body — zero-copy.
                let mut src = BufferReader::new();
                src.push(Buffer::Host(h));
                M::decode(&mut src).map_err(|e| DecodingError::DecodeMessage(ty, e))
            }
        }
    }

    pub fn decode_as_notification(self) -> Result<Notification, DecodingError> {
        let ty = self.ty();
        assert!(ty.is_notification(), "Expected a notification");

        let messages::NotificationTemplate { id, result } = match self.1 {
            Buffer::InMemory(b) => messages::NotificationTemplate::decode(b)
                .map_err(|e| DecodingError::DecodeMessage(self.0.message_type(), e))?,
            Buffer::Host(h) => {
                let mut src = BufferReader::new();
                src.push(Buffer::Host(h));
                messages::NotificationTemplate::decode(&mut src)
                    .map_err(|e| DecodingError::DecodeMessage(self.0.message_type(), e))?
            }
        };

        Ok(Notification {
            id: id.ok_or(DecodingError::MissingField {
                expected: ty,
                field: "id",
            })?,
            result: result.ok_or(DecodingError::MissingField {
                expected: ty,
                field: "result",
            })?,
        })
    }
}

/// Stateful decoder to decode [`RestateMessage`]s out of a stream of
/// host-aware input buffers.
///
/// Owns one [`BufferReader`] — the protocol decoder appends new segments
/// via [`Self::push`] and reads the 8-byte header off it via `Buf`. Each
/// message body is extracted as a [`Buffer`] (single-segment) and
/// returned in a [`RawMessage`]. When the body fits entirely within a
/// single head host segment the body is a `Buffer::Host(sub_view)`
/// (zero-copy); cross-segment bodies and inline bodies materialise into
/// `Buffer::InMemory`.
pub struct Decoder {
    reader: BufferReader,
    state: DecoderState,
}

impl Decoder {
    pub fn new(service_protocol_version: Version) -> Self {
        assert!(
            service_protocol_version >= Version::minimum_supported_version(),
            "Decoder only supports service protocol version {:?} <= x <= {:?}",
            Version::minimum_supported_version(),
            Version::maximum_supported_version()
        );
        Self {
            reader: BufferReader::new(),
            state: DecoderState::WaitingHeader,
        }
    }

    /// Append a new buffer segment to the internal reader.
    pub fn push(&mut self, buf: impl Into<Buffer>) {
        self.reader.push(buf);
    }

    /// Try to consume the next protocol.message in the internal buffer.
    pub fn consume_next(&mut self) -> Result<Option<RawMessage>, DecodingError> {
        loop {
            match self.state {
                DecoderState::WaitingHeader => {
                    if self.reader.remaining() < 8 {
                        return Ok(None);
                    }
                    let header: MessageHeader = self.reader.get_u64().try_into()?;
                    self.state = DecoderState::WaitingPayload(header);
                }
                DecoderState::WaitingPayload(header) => {
                    let body_len = header.message_length() as usize;
                    if self.reader.remaining() < body_len {
                        return Ok(None);
                    }
                    let body = take_body(&mut self.reader, body_len);
                    self.state = DecoderState::WaitingHeader;
                    return Ok(Some(RawMessage(header, body)));
                }
            }
        }
    }
}

/// Peel a body of exactly `len` bytes off the reader, preferring the
/// zero-copy single-segment path.
///
/// - **Single-segment Host**: mint a sub-view and advance — zero-copy.
/// - **Single-segment InMemory**: `Bytes::split_to` — zero-copy.
/// - **Cross-segment**: materialise into a fresh `Bytes` via the
///   `Buf::copy_to_bytes` path.
fn take_body(reader: &mut BufferReader, len: usize) -> Buffer {
    if len == 0 {
        return Buffer::InMemory(Bytes::new());
    }
    // Single-segment host fast path.
    if let Some(handle) = reader.try_take_host_slice(len) {
        return Buffer::Host(handle);
    }
    // Single-segment InMemory or cross-segment: defer to `copy_to_bytes`.
    // The `BufferReader::copy_to_bytes` impl picks the zero-copy
    // `Bytes::split_to` path when the head is an InMemory segment with
    // enough bytes, otherwise materialises.
    Buffer::InMemory(reader.copy_to_bytes(len))
}

#[derive(Copy, Clone, Debug)]
enum DecoderState {
    WaitingHeader,
    WaitingPayload(MessageHeader),
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::tests::Encoder;

    #[test]
    fn fill_decoder_with_several_messages() {
        let encoder = Encoder::new(Version::maximum_supported_version());
        let mut decoder = Decoder::new(Version::maximum_supported_version());

        let expected_msg_0 = messages::StartMessage {
            id: "key".into(),
            debug_id: "key".into(),
            known_entries: 1,
            state_map: vec![],
            partial_state: true,
            key: "key".to_string(),
            retry_count_since_last_stored_entry: 0,
            duration_since_last_stored_entry: 0,
            random_seed: 0,
            scope: None,
            limit_key: None,
            idempotency_key: None,
        };

        let expected_msg_1 = messages::InputCommandMessage {
            value: Some(messages::Value {
                content: Buffer::InMemory(Bytes::from_static("input".as_bytes())),
            }),
            ..messages::InputCommandMessage::default()
        };
        let expected_msg_2 = messages::GetLazyStateCompletionNotificationMessage {
            completion_id: 1,
            result: Some(
                messages::get_lazy_state_completion_notification_message::Result::Void(
                    messages::Void::default(),
                ),
            ),
        };

        decoder.push(encoder.encode(&expected_msg_0));
        decoder.push(encoder.encode(&expected_msg_1));
        decoder.push(encoder.encode(&expected_msg_2));

        let actual_msg_0 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_0.ty(), MessageType::Start);
        assert_eq!(
            actual_msg_0.decode_to::<messages::StartMessage>(0).unwrap(),
            expected_msg_0
        );

        let actual_msg_1 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_1.header().message_type(),
            MessageType::InputCommand
        );
        assert_eq!(
            actual_msg_1
                .decode_to::<messages::InputCommandMessage>(1)
                .unwrap(),
            expected_msg_1
        );

        let actual_msg_2 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_2.header().message_type(),
            MessageType::GetLazyStateCompletionNotification
        );
        assert_eq!(
            actual_msg_2
                .decode_to::<messages::GetLazyStateCompletionNotificationMessage>(2)
                .unwrap(),
            expected_msg_2
        );

        assert!(decoder.consume_next().unwrap().is_none());
    }

    #[test]
    fn fill_decoder_with_partial_header() {
        partial_decoding_test(4)
    }

    #[test]
    fn fill_decoder_with_partial_body() {
        partial_decoding_test(10)
    }

    fn partial_decoding_test(split_index: usize) {
        let encoder = Encoder::new(Version::maximum_supported_version());
        let mut decoder = Decoder::new(Version::maximum_supported_version());

        let expected_msg = messages::InputCommandMessage {
            value: Some(messages::Value {
                content: Buffer::InMemory(Bytes::from_static("input".as_bytes())),
            }),
            ..messages::InputCommandMessage::default()
        };
        let expected_msg_encoded = encoder.encode(&expected_msg);

        decoder.push(expected_msg_encoded.slice(0..split_index));
        assert!(decoder.consume_next().unwrap().is_none());

        decoder.push(expected_msg_encoded.slice(split_index..));

        let actual_msg = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg.header().message_type(),
            MessageType::InputCommand
        );
        assert_eq!(
            actual_msg
                .decode_to::<messages::InputCommandMessage>(4)
                .unwrap(),
            expected_msg
        );

        assert!(decoder.consume_next().unwrap().is_none());
    }
}
