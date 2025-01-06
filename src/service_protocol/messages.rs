use crate::service_protocol::{MessageHeader, MessageType, NotificationResult};
use bytes::Bytes;
use paste::paste;
use std::borrow::Cow;

pub trait RestateMessage: prost::Message + Default {
    fn ty() -> MessageType;

    fn generate_header(&self) -> MessageHeader {
        MessageHeader::new(Self::ty(), self.encoded_len() as u32)
    }
}

pub trait NamedCommandMessage {
    fn name(&self) -> String;
}

pub trait CommandMessageHeaderEq {
    fn header_eq(&self, other: &Self) -> bool;
}

include!("./generated/dev.restate.service.protocol.rs");

macro_rules! impl_message_traits {
    ($name:ident: core) => {
        impl_message_traits!($name: message);
    };
    ($name:ident: notification) => {
        impl_message_traits!($name: message);
    };
    ($name:ident: command) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: named_command);
        impl_message_traits!($name: command_header_eq);
    };
    ($name:ident: message) => {
         impl RestateMessage for paste! { [<$name Message>] } {
            fn ty() -> MessageType {
                MessageType::$name
            }
        }
    };
    ($name:ident: named_command) => {
        impl NamedCommandMessage for paste! { [<$name Message>] } {
            fn name(&self) -> String {
                self.name.clone()
            }
        }
    };
    ($name:ident: command_header_eq) => {
        impl CommandMessageHeaderEq for paste! { [<$name Message>] } {
            fn header_eq(&self, other: &Self) -> bool {
                self.eq(other)
            }
        }
    };
}

// --- Core messages
impl_message_traits!(Start: core);
impl_message_traits!(Suspension: core);
impl_message_traits!(Error: core);
impl_message_traits!(End: core);
impl_message_traits!(ProposeRunCompletion: core);

// -- Entries
impl_message_traits!(InputCommand: message);
impl_message_traits!(InputCommand: named_command);
impl CommandMessageHeaderEq for InputCommandMessage {
    fn header_eq(&self, _: &Self) -> bool {
        true
    }
}

impl_message_traits!(OutputCommand: command);
impl_message_traits!(GetLazyStateCommand: command);
impl_message_traits!(GetLazyStateCompletionNotification: notification);
impl_message_traits!(SetStateCommand: command);
impl_message_traits!(ClearStateCommand: command);
impl_message_traits!(ClearAllStateCommand: command);
impl_message_traits!(GetLazyStateKeysCommand: command);
impl_message_traits!(GetLazyStateKeysCompletionNotification: notification);
impl_message_traits!(GetEagerStateCommand: command);
impl_message_traits!(GetEagerStateKeysCommand: command);
impl_message_traits!(GetPromiseCommand: command);
impl_message_traits!(GetPromiseCompletionNotification: notification);
impl_message_traits!(PeekPromiseCommand: command);
impl_message_traits!(PeekPromiseCompletionNotification: notification);
impl_message_traits!(CompletePromiseCommand: command);
impl_message_traits!(CompletePromiseCompletionNotification: notification);

impl_message_traits!(SleepCommand: message);
impl_message_traits!(SleepCommand: named_command);
impl CommandMessageHeaderEq for SleepCommandMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl_message_traits!(SleepCompletionNotification: notification);
impl_message_traits!(CallCommand: command);
impl_message_traits!(CallInvocationIdCompletionNotification: notification);
impl_message_traits!(CallCompletionNotification: notification);

impl_message_traits!(OneWayCallCommand: message);
impl_message_traits!(OneWayCallCommand: named_command);
impl CommandMessageHeaderEq for OneWayCallCommandMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.service_name == other.service_name
            && self.handler_name == other.handler_name
            && self.key == other.key
            && self.headers == other.headers
            && self.parameter == other.parameter
            && self.name == other.name
    }
}

impl_message_traits!(SendSignalCommand: message);
impl NamedCommandMessage for SendSignalCommandMessage {
    fn name(&self) -> String {
        self.entry_name.clone()
    }
}
impl_message_traits!(SendSignalCommand: command_header_eq);

impl_message_traits!(RunCommand: command);
impl_message_traits!(RunCompletionNotification: notification);
impl_message_traits!(AttachInvocationCommand: command);
impl_message_traits!(AttachInvocationCompletionNotification: notification);
impl_message_traits!(GetInvocationOutputCommand: command);
impl_message_traits!(GetInvocationOutputCompletionNotification: notification);
impl_message_traits!(CompleteAwakeableCommand: command);
impl_message_traits!(SignalNotification: notification);

// --- Other conversions

impl From<crate::TerminalFailure> for Failure {
    fn from(value: crate::TerminalFailure) -> Self {
        Self {
            code: value.code as u32,
            message: value.message,
        }
    }
}

impl From<Failure> for crate::TerminalFailure {
    fn from(value: Failure) -> Self {
        Self {
            code: value.code as u16,
            message: value.message,
        }
    }
}

impl From<Header> for crate::Header {
    fn from(header: Header) -> Self {
        Self {
            key: Cow::Owned(header.key),
            value: Cow::Owned(header.value),
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

impl From<Bytes> for Value {
    fn from(content: Bytes) -> Self {
        Self { content }
    }
}

impl From<NotificationResult> for crate::Value {
    fn from(value: NotificationResult) -> Self {
        match value {
            NotificationResult::Void(_) => crate::Value::Void,
            NotificationResult::Value(v) => crate::Value::Success(v.content),
            NotificationResult::Failure(f) => crate::Value::Failure(f.into()),
            NotificationResult::InvocationId(id) => crate::Value::InvocationId(id),
            NotificationResult::StateKeys(sk) => crate::Value::StateKeys(
                sk.keys
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect(),
            ),
        }
    }
}
