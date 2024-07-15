use crate::service_protocol::{MessageHeader, MessageType};
use crate::{NonEmptyValue, Value};
use paste::paste;
use prost::Message;

pub trait RestateMessage: prost::Message + Default {
    fn ty() -> MessageType;
}

pub trait WriteableRestateMessage: RestateMessage {
    fn generate_header(&self, _never_ack: bool) -> MessageHeader {
        MessageHeader::new(Self::ty(), self.encoded_len() as u32)
    }
}

pub trait EntryMessage {
    fn name(&self) -> String;
}

pub trait EntryMessageHeaderEq {
    fn header_eq(&self, other: &Self) -> bool;
}

pub trait CompletableEntryMessage: RestateMessage + EntryMessage + EntryMessageHeaderEq {
    fn is_completed(&self) -> bool;
    fn into_completion(self) -> Option<Value>;
}

impl<M: CompletableEntryMessage> WriteableRestateMessage for M {
    fn generate_header(&self, _never_ack: bool) -> MessageHeader {
        MessageHeader::new_entry_header(
            Self::ty(),
            Some(self.is_completed()),
            self.encoded_len() as u32,
        )
    }
}

include!("./generated/dev.restate.service.protocol.rs");

macro_rules! impl_message_traits {
    ($name:ident: core) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: writeable);
    };
    ($name:ident: non_completable_entry) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: writeable);
        impl_message_traits!($name: entry);
        impl_message_traits!($name: entry_header_eq);
    };
    ($name:ident: completable_entry) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: entry);
        impl_message_traits!($name: completable);
    };
    ($name:ident: message) => {
         impl RestateMessage for paste! { [<$name Message>] } {
            fn ty() -> MessageType {
                MessageType::$name
            }
        }
    };
    ($name:ident: writeable) => {
        impl WriteableRestateMessage for paste! { [<$name Message>] } {}
    };
    ($name:ident: completable) => {
        impl CompletableEntryMessage for paste! { [<$name Message>] } {
            fn is_completed(&self) -> bool {
                self.result.is_some()
            }

            fn into_completion(self) -> Option<Value> {
                self.result.map(Into::into)
            }
        }
    };
    ($name:ident: entry) => {
        impl EntryMessage for paste! { [<$name Message>] } {
            fn name(&self) -> String {
                self.name.clone()
            }
        }
    };
    ($name:ident: entry_header_eq) => {
        impl EntryMessageHeaderEq for paste! { [<$name Message>] } {
            fn header_eq(&self, other: &Self) -> bool {
                self.eq(other)
            }
        }
    };
}

// --- Core messages
impl_message_traits!(Start: core);
impl_message_traits!(Completion: core);
impl_message_traits!(Suspension: core);
impl_message_traits!(Error: core);
impl_message_traits!(End: core);
impl_message_traits!(EntryAck: core);

// -- Entries
impl_message_traits!(InputEntry: message);
impl_message_traits!(InputEntry: entry);
impl_message_traits!(InputEntry: writeable);
impl EntryMessageHeaderEq for InputEntryMessage {
    fn header_eq(&self, _: &Self) -> bool {
        true
    }
}

impl_message_traits!(OutputEntry: non_completable_entry);

impl_message_traits!(GetStateEntry: completable_entry);
impl EntryMessageHeaderEq for GetStateEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl_message_traits!(SetStateEntry: non_completable_entry);

impl_message_traits!(ClearStateEntry: non_completable_entry);

impl_message_traits!(ClearAllStateEntry: non_completable_entry);

impl_message_traits!(SleepEntry: completable_entry);
impl EntryMessageHeaderEq for SleepEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl_message_traits!(CallEntry: completable_entry);
impl EntryMessageHeaderEq for CallEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.service_name == other.service_name
            && self.handler_name == other.handler_name
            && self.key == other.key
            && self.headers == other.headers
            && self.parameter == other.parameter
            && self.name == other.name
    }
}

impl_message_traits!(OneWayCallEntry: message);
impl_message_traits!(OneWayCallEntry: writeable);
impl_message_traits!(OneWayCallEntry: entry);
impl EntryMessageHeaderEq for OneWayCallEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.service_name == other.service_name
            && self.handler_name == other.handler_name
            && self.key == other.key
            && self.headers == other.headers
            && self.parameter == other.parameter
            && self.name == other.name
    }
}

impl_message_traits!(AwakeableEntry: completable_entry);
impl EntryMessageHeaderEq for AwakeableEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl_message_traits!(CompleteAwakeableEntry: non_completable_entry);

impl_message_traits!(GetPromiseEntry: completable_entry);
impl EntryMessageHeaderEq for GetPromiseEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.key == other.key && self.name == other.name
    }
}

impl_message_traits!(PeekPromiseEntry: completable_entry);
impl EntryMessageHeaderEq for PeekPromiseEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.key == other.key && self.name == other.name
    }
}

impl_message_traits!(CompletePromiseEntry: completable_entry);
impl EntryMessageHeaderEq for CompletePromiseEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.key == other.key && self.completion == other.completion && self.name == other.name
    }
}

impl_message_traits!(RunEntry: message);
impl_message_traits!(RunEntry: entry);
impl WriteableRestateMessage for RunEntryMessage {
    fn generate_header(&self, never_ack: bool) -> MessageHeader {
        MessageHeader::new_ackable_entry_header(
            MessageType::RunEntry,
            None,
            if never_ack { Some(false) } else { Some(true) },
            self.encoded_len() as u32,
        )
    }
}
impl EntryMessageHeaderEq for RunEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

// --- Completion extraction

impl From<completion_message::Result> for Value {
    fn from(value: completion_message::Result) -> Self {
        match value {
            completion_message::Result::Empty(_) => Value::Void,
            completion_message::Result::Value(b) => Value::Success(b.to_vec()),
            completion_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<get_state_entry_message::Result> for Value {
    fn from(value: get_state_entry_message::Result) -> Self {
        match value {
            get_state_entry_message::Result::Empty(_) => Value::Void,
            get_state_entry_message::Result::Value(b) => Value::Success(b.into()),
            get_state_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<sleep_entry_message::Result> for Value {
    fn from(value: sleep_entry_message::Result) -> Self {
        match value {
            sleep_entry_message::Result::Empty(_) => Value::Void,
            sleep_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<call_entry_message::Result> for Value {
    fn from(value: call_entry_message::Result) -> Self {
        match value {
            call_entry_message::Result::Value(b) => Value::Success(b.into()),
            call_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<awakeable_entry_message::Result> for Value {
    fn from(value: awakeable_entry_message::Result) -> Self {
        match value {
            awakeable_entry_message::Result::Value(b) => Value::Success(b.into()),
            awakeable_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<get_promise_entry_message::Result> for Value {
    fn from(value: get_promise_entry_message::Result) -> Self {
        match value {
            get_promise_entry_message::Result::Value(b) => Value::Success(b.into()),
            get_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<peek_promise_entry_message::Result> for Value {
    fn from(value: peek_promise_entry_message::Result) -> Self {
        match value {
            peek_promise_entry_message::Result::Empty(_) => Value::Void,
            peek_promise_entry_message::Result::Value(b) => Value::Success(b.into()),
            peek_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<complete_promise_entry_message::Result> for Value {
    fn from(value: complete_promise_entry_message::Result) -> Self {
        match value {
            complete_promise_entry_message::Result::Empty(_) => Value::Void,
            complete_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        }
    }
}

impl From<run_entry_message::Result> for NonEmptyValue {
    fn from(value: run_entry_message::Result) -> Self {
        match value {
            run_entry_message::Result::Value(b) => NonEmptyValue::Success(b.into()),
            run_entry_message::Result::Failure(f) => NonEmptyValue::Failure(f.into()),
        }
    }
}

// --- Other conversions

impl From<crate::Failure> for Failure {
    fn from(value: crate::Failure) -> Self {
        Self {
            code: value.code as u32,
            message: value.message,
        }
    }
}

impl From<Failure> for crate::Failure {
    fn from(value: Failure) -> Self {
        Self {
            code: value.code as u16,
            message: value.message,
        }
    }
}
