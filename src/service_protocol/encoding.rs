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

use std::mem;

use crate::vm::errors::CommandTypeMismatchError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use prost::Message;

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

// --- Input protocol.message encoder

pub struct Encoder {}

impl Encoder {
    pub fn new(service_protocol_version: Version) -> Self {
        assert!(
            service_protocol_version >= Version::minimum_supported_version(),
            "Encoder only supports service protocol version {:?} <= x <= {:?}",
            Version::minimum_supported_version(),
            Version::maximum_supported_version()
        );
        Self {}
    }

    /// Encodes a protocol message to bytes
    pub fn encode<M: RestateMessage>(&self, msg: &M) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_len(msg));
        self.encode_to_buf_mut(&mut buf, msg).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );
        buf.freeze()
    }

    /// Includes header len
    pub fn encoded_len<M: RestateMessage>(&self, msg: &M) -> usize {
        8 + msg.encoded_len()
    }

    pub fn encode_to_buf_mut<M: RestateMessage>(
        &self,
        mut buf: impl BufMut,
        msg: &M,
    ) -> Result<(), prost::EncodeError> {
        let header = msg.generate_header();
        buf.put_u64(header.into());
        // Note:
        // prost::EncodeError can be triggered only by a buffer smaller than required,
        // but because we create the buffer a couple of lines above using the size computed by prost,
        // this can happen only if there is a very bad bug in prost.
        msg.encode(&mut buf)
    }
}

// --- Input protocol.message decoder

#[derive(Debug, Clone, PartialEq)]
pub struct RawMessage(MessageHeader, Bytes);

impl RawMessage {
    pub fn ty(&self) -> MessageType {
        self.0.message_type()
    }

    pub fn decode_to<M: RestateMessage>(self, command_index: i64) -> Result<M, DecodingError> {
        if self.0.message_type() != M::ty() {
            return Err(DecodingError::UnexpectedMessageType(
                CommandTypeMismatchError::new(command_index, self.0.message_type(), M::ty()),
            ));
        }
        M::decode(self.1).map_err(|e| DecodingError::DecodeMessage(self.0.message_type(), e))
    }

    pub fn decode_as_notification(self) -> Result<Notification, DecodingError> {
        let ty = self.ty();
        assert!(ty.is_notification(), "Expected a notification");

        let messages::NotificationTemplate { id, result } =
            messages::NotificationTemplate::decode(self.1)
                .map_err(|e| DecodingError::DecodeMessage(self.0.message_type(), e))?;

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

/// Stateful decoder to decode [`RestateMessage`]
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
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
            buf: SegmentedBuf::new(),
            state: DecoderState::WaitingHeader,
        }
    }

    /// Concatenate a new chunk in the internal buffer.
    pub fn push(&mut self, buf: Bytes) {
        self.buf.push(buf)
    }

    /// Try to consume the next protocol.message in the internal buffer.
    pub fn consume_next(&mut self) -> Result<Option<RawMessage>, DecodingError> {
        loop {
            let remaining = self.buf.remaining();

            if remaining < self.state.needs_bytes() {
                return Ok(None);
            }

            if let Some(res) = self.state.decode(&mut self.buf)? {
                return Ok(Some(res));
            }
        }
    }
}

#[derive(Default)]
enum DecoderState {
    #[default]
    WaitingHeader,
    WaitingPayload(MessageHeader),
}

impl DecoderState {
    fn needs_bytes(&self) -> usize {
        match self {
            DecoderState::WaitingHeader => 8,
            DecoderState::WaitingPayload(h) => h.message_length() as usize,
        }
    }

    fn decode(&mut self, mut buf: impl Buf) -> Result<Option<RawMessage>, DecodingError> {
        let mut res = None;

        *self = match mem::take(self) {
            DecoderState::WaitingHeader => {
                let header: MessageHeader = buf.get_u64().try_into()?;
                DecoderState::WaitingPayload(header)
            }
            DecoderState::WaitingPayload(h) => {
                let msg = RawMessage(h, buf.copy_to_bytes(h.message_length() as usize));
                res = Some(msg);
                DecoderState::WaitingHeader
            }
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    impl RawMessage {
        pub fn header(&self) -> MessageHeader {
            self.0
        }
    }

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
        };

        let expected_msg_1 = messages::InputCommandMessage {
            value: Some(messages::Value {
                content: Bytes::from_static("input".as_bytes()),
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
                content: Bytes::from_static("input".as_bytes()),
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
