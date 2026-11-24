//! The explicit recovery protocol family: its versions, its message type registry and its
//! messages. The framing (header, decoder) is shared with the durable execution protocol.

mod header;
pub(crate) mod messages;
mod version;

pub(crate) use header::MessageType;
pub use version::ProtocolVersion;

/// The decoder of the family's messages.
pub(crate) type Decoder = super::Decoder<MessageType>;
pub(crate) type RawMessage = super::RawMessage<MessageType>;
#[allow(dead_code)]
pub(crate) type MessageHeader = super::MessageHeader<MessageType>;
