//! The messages of the explicit recovery protocol, generated from `explicit_recovery.proto`,
//! and what ties each to its type in the registry, as `service_protocol::messages` does for the
//! durable execution protocol.
//!
//! The generated code keeps the proto's comments, whose continuation lines clippy finds
//! overindented: the lint is allowed for this module, and the proto stays as it is.
#![allow(clippy::doc_overindented_list_items)]

use super::MessageType;
use crate::service_protocol::MessageHeader;
use crate::Error;
use bytes::{BufMut, Bytes, BytesMut};
use pastey::paste;

/// A message of the family, with its type.
pub trait RestateMessage: prost::Message + Default {
    fn ty() -> MessageType;
}

include!("../generated/dev.restate.service.explicitrecovery.rs");

macro_rules! impl_message {
    ($($name:ident,)*) => {
        $(
            impl RestateMessage for paste! { [<$name Message>] } {
                fn ty() -> MessageType {
                    MessageType::$name
                }
            }
        )*
    };
}

impl_message!(
    Start,
    Shutdown,
    Error,
    End,
    Heartbeat,
    GetStateAction,
    GetStateKeysAction,
    SetStateAction,
    ClearStateAction,
    ClearAllStateAction,
    CallAction,
    AttachInvocationAction,
    CancelInvocationAction,
    ReadSignalAction,
    SendSignalAction,
    FlushAction,
    AtomicGroupAction,
    SpanStartAction,
    SpanEndAction,
    ResultNotification,
    SignalNotification,
);

/// An action an atomic group can hold.
pub trait GroupableAction: RestateMessage {
    fn into_group_action(self) -> atomic_group_action_message::Action;
}

macro_rules! impl_groupable {
    ($($variant:ident => $name:ident,)*) => {
        $(
            impl GroupableAction for $name {
                fn into_group_action(self) -> atomic_group_action_message::Action {
                    atomic_group_action_message::Action {
                        action: Some(atomic_group_action_message::action::Action::$variant(self)),
                    }
                }
            }
        )*
    };
}

impl_groupable!(
    SetState => SetStateActionMessage,
    ClearState => ClearStateActionMessage,
    ClearAllState => ClearAllStateActionMessage,
    Call => CallActionMessage,
    CancelInvocation => CancelInvocationActionMessage,
    SendSignal => SendSignalActionMessage,
    SpanStart => SpanStartActionMessage,
    SpanEnd => SpanEndActionMessage,
);

/// The message with its header: its type, 16 reserved bits (zero), its length.
pub(crate) fn encode<M: RestateMessage>(msg: &M) -> Bytes {
    let len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(8 + len);
    buf.put_u64(MessageHeader::new_plain(M::ty(), len as u32).into());
    msg.encode(&mut buf)
        .expect("the buffer has the size prost computed");
    buf.freeze()
}

// --- Conversions

impl From<Bytes> for Value {
    fn from(content: Bytes) -> Self {
        Self { content }
    }
}

impl From<crate::TerminalFailure> for Failure {
    fn from(value: crate::TerminalFailure) -> Self {
        Self {
            code: value.code as u32,
            message: value.message,
            metadata: value
                .metadata
                .into_iter()
                .map(|(key, value)| FailureMetadata { key, value })
                .collect(),
        }
    }
}

impl From<Failure> for crate::TerminalFailure {
    fn from(value: Failure) -> Self {
        Self {
            code: value.code as u16,
            message: value.message,
            metadata: value
                .metadata
                .into_iter()
                .map(|fm| (fm.key, fm.value))
                .collect(),
        }
    }
}

impl From<Header> for crate::Header {
    fn from(header: Header) -> Self {
        Self {
            key: header.key.into(),
            value: header.value.into(),
        }
    }
}

impl From<crate::Header> for Header {
    fn from(header: crate::Header) -> Self {
        Self {
            key: header.key.into(),
            value: header.value.into(),
        }
    }
}

impl From<result_notification_message::Result> for crate::Value {
    fn from(value: result_notification_message::Result) -> Self {
        use result_notification_message::Result;
        match value {
            Result::Void(_) => crate::Value::Void,
            Result::Value(v) => crate::Value::Success(v.content),
            Result::Failure(f) => crate::Value::Failure(f.into()),
            Result::InvocationId(id) => crate::Value::InvocationId(id),
            Result::StateKeys(sk) => crate::Value::StateKeys(
                sk.keys
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect(),
            ),
        }
    }
}

impl From<crate::service_protocol::messages::ErrorBehavior> for ErrorBehavior {
    fn from(value: crate::service_protocol::messages::ErrorBehavior) -> Self {
        use crate::service_protocol::messages::ErrorBehavior as Other;
        match value {
            Other::Retry => ErrorBehavior::Retry,
            Other::Pause => ErrorBehavior::Pause,
            Other::Fail => ErrorBehavior::Fail,
        }
    }
}

/// Unlike the durable execution protocol's, the message carries the error's metadata: a missing
/// feature (573) is named there.
impl From<&Error> for ErrorMessage {
    fn from(value: &Error) -> ErrorMessage {
        Self {
            code: value.code as u32,
            message: value.message.clone().into_owned(),
            stacktrace: value.stacktrace.clone(),
            // Not filled: `Error` names a related command of the durable execution protocol only.
            related_action_type: None,
            related_action_name: None,
            related_result_id: None,
            next_retry_delay: value
                .next_retry_delay
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            behavior: ErrorBehavior::from(value.behavior) as i32,
            metadata: value
                .metadata
                .iter()
                .map(|(key, value)| FailureMetadata {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}
