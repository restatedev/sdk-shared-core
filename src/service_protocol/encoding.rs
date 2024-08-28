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
use super::messages::{RestateMessage, WriteableRestateMessage};
use super::*;

use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;

#[derive(Debug, thiserror::Error)]
pub enum DecodingError {
    #[error("cannot decode protocol message type {0:?}. Reason: {1:?}")]
    DecodeMessage(MessageType, #[source] prost::DecodeError),
    #[error("expected message type {expected:?} but was {actual:?}")]
    UnexpectedMessageType {
        expected: MessageType,
        actual: MessageType,
    },
    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),
}

// --- Input protocol.message encoder

pub struct Encoder {}

impl Encoder {
    pub fn new(service_protocol_version: Version) -> Self {
        assert_eq!(
            service_protocol_version,
            Version::latest(),
            "Encoder only supports service protocol version {:?}",
            Version::latest()
        );
        Self {}
    }

    /// Encodes a protocol message to bytes
    pub fn encode<M: WriteableRestateMessage>(&self, msg: &M) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_len(msg));
        self.encode_to_buf_mut(&mut buf, msg).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );
        buf.freeze()
    }

    /// Includes header len
    pub fn encoded_len<M: WriteableRestateMessage>(&self, msg: &M) -> usize {
        8 + msg.encoded_len()
    }

    pub fn encode_to_buf_mut<M: WriteableRestateMessage>(
        &self,
        mut buf: impl BufMut,
        msg: &M,
    ) -> Result<(), prost::EncodeError> {
        let header = msg.generate_header(false);
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

    pub fn decode_to<M: RestateMessage>(self) -> Result<M, DecodingError> {
        if self.0.message_type() != M::ty() {
            return Err(DecodingError::UnexpectedMessageType {
                expected: M::ty(),
                actual: self.0.message_type(),
            });
        }
        M::decode(self.1).map_err(|e| DecodingError::DecodeMessage(self.0.message_type(), e))
    }
}

/// Stateful decoder to decode [`RestateMessage`]
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
    state: DecoderState,
}

impl Decoder {
    pub fn new(service_protocol_version: Version) -> Self {
        assert_eq!(
            service_protocol_version,
            Version::latest(),
            "Decoder only supports service protocol version {:?}",
            Version::latest()
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
            DecoderState::WaitingPayload(h) => h.frame_length() as usize,
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
                let msg = RawMessage(h, buf.copy_to_bytes(h.frame_length() as usize));
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
        let encoder = Encoder::new(Version::latest());
        let mut decoder = Decoder::new(Version::latest());

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

        let expected_msg_1 = messages::InputEntryMessage {
            value: Bytes::from_static("input".as_bytes()),
            ..messages::InputEntryMessage::default()
        };
        let expected_msg_2 = messages::CompletionMessage {
            entry_index: 1,
            result: Some(messages::completion_message::Result::Empty(
                messages::Empty::default(),
            )),
        };

        decoder.push(encoder.encode(&expected_msg_0));
        decoder.push(encoder.encode(&expected_msg_1));
        decoder.push(encoder.encode(&expected_msg_2));

        let actual_msg_0 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_0.ty(), MessageType::Start);
        assert_eq!(
            actual_msg_0.decode_to::<messages::StartMessage>().unwrap(),
            expected_msg_0
        );

        let actual_msg_1 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_1.header().message_type(),
            MessageType::InputEntry
        );
        assert_eq!(actual_msg_1.header().completed(), None);
        assert_eq!(
            actual_msg_1
                .decode_to::<messages::InputEntryMessage>()
                .unwrap(),
            expected_msg_1
        );

        let actual_msg_2 = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_2.header().message_type(),
            MessageType::Completion
        );
        assert_eq!(
            actual_msg_2
                .decode_to::<messages::CompletionMessage>()
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
        let encoder = Encoder::new(Version::latest());
        let mut decoder = Decoder::new(Version::latest());

        let expected_msg = messages::InputEntryMessage {
            value: Bytes::from_static("input".as_bytes()),
            ..messages::InputEntryMessage::default()
        };
        let expected_msg_encoded = encoder.encode(&expected_msg);

        decoder.push(expected_msg_encoded.slice(0..split_index));
        assert!(decoder.consume_next().unwrap().is_none());

        decoder.push(expected_msg_encoded.slice(split_index..));

        let actual_msg = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg.header().message_type(), MessageType::InputEntry);
        assert_eq!(actual_msg.header().completed(), None);
        assert_eq!(
            actual_msg
                .decode_to::<messages::InputEntryMessage>()
                .unwrap(),
            expected_msg
        );

        assert!(decoder.consume_next().unwrap().is_none());
    }
}
