use crate::service_protocol::messages::get_state_keys_entry_message::StateKeys;
use crate::service_protocol::{MessageHeader, MessageType};
use crate::vm::errors::{
    DecodeGetCallInvocationIdUtf8, DecodeStateKeysProst, DecodeStateKeysUtf8,
    EmptyGetCallInvocationId, EmptyStateKeys,
};
use crate::{Error, NonEmptyValue, Value};
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
    fn into_completion(self) -> Result<Option<Value>, Error>;
    fn completion_parsing_hint() -> CompletionParsingHint;
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
include!("./generated/dev.restate.service.protocol.extensions.rs");

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

            fn into_completion(self) -> Result<Option<Value>, Error> {
                self.result.map(TryInto::try_into).transpose()
            }

            fn completion_parsing_hint() -> CompletionParsingHint {
                CompletionParsingHint::EmptyOrSuccessOrValue
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

impl_message_traits!(GetStateKeysEntry: message);
impl_message_traits!(GetStateKeysEntry: entry);
impl CompletableEntryMessage for GetStateKeysEntryMessage {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }

    fn into_completion(self) -> Result<Option<Value>, Error> {
        self.result.map(TryInto::try_into).transpose()
    }

    fn completion_parsing_hint() -> CompletionParsingHint {
        CompletionParsingHint::StateKeys
    }
}
impl EntryMessageHeaderEq for GetStateKeysEntryMessage {
    fn header_eq(&self, _: &Self) -> bool {
        true
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

impl_message_traits!(CancelInvocationEntry: non_completable_entry);

impl_message_traits!(GetCallInvocationIdEntry: message);
impl_message_traits!(GetCallInvocationIdEntry: entry);
impl CompletableEntryMessage for GetCallInvocationIdEntryMessage {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }

    fn into_completion(self) -> Result<Option<Value>, Error> {
        self.result.map(TryInto::try_into).transpose()
    }

    fn completion_parsing_hint() -> CompletionParsingHint {
        CompletionParsingHint::GetCompletionId
    }
}
impl EntryMessageHeaderEq for GetCallInvocationIdEntryMessage {
    fn header_eq(&self, other: &Self) -> bool {
        self.call_entry_index == other.call_entry_index
    }
}

impl_message_traits!(CombinatorEntry: message);
impl_message_traits!(CombinatorEntry: entry);
impl WriteableRestateMessage for CombinatorEntryMessage {
    fn generate_header(&self, never_ack: bool) -> MessageHeader {
        MessageHeader::new_ackable_entry_header(
            MessageType::CombinatorEntry,
            None,
            if never_ack { Some(false) } else { Some(true) },
            self.encoded_len() as u32,
        )
    }
}
impl EntryMessageHeaderEq for CombinatorEntryMessage {
    fn header_eq(&self, _: &Self) -> bool {
        true
    }
}

// --- Completion extraction

impl TryFrom<get_state_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: get_state_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            get_state_entry_message::Result::Empty(_) => Value::Void,
            get_state_entry_message::Result::Value(b) => Value::Success(b),
            get_state_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<get_state_keys_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: get_state_keys_entry_message::Result) -> Result<Self, Self::Error> {
        match value {
            get_state_keys_entry_message::Result::Value(state_keys) => {
                let mut state_keys = state_keys
                    .keys
                    .into_iter()
                    .map(|b| String::from_utf8(b.to_vec()).map_err(DecodeStateKeysUtf8))
                    .collect::<Result<Vec<_>, _>>()?;
                state_keys.sort();
                Ok(Value::StateKeys(state_keys))
            }
            get_state_keys_entry_message::Result::Failure(f) => Ok(Value::Failure(f.into())),
        }
    }
}

impl TryFrom<sleep_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: sleep_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            sleep_entry_message::Result::Empty(_) => Value::Void,
            sleep_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<call_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: call_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            call_entry_message::Result::Value(b) => Value::Success(b),
            call_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<awakeable_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: awakeable_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            awakeable_entry_message::Result::Value(b) => Value::Success(b),
            awakeable_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<get_promise_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: get_promise_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            get_promise_entry_message::Result::Value(b) => Value::Success(b),
            get_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<peek_promise_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: peek_promise_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            peek_promise_entry_message::Result::Empty(_) => Value::Void,
            peek_promise_entry_message::Result::Value(b) => Value::Success(b),
            peek_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl TryFrom<complete_promise_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: complete_promise_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            complete_promise_entry_message::Result::Empty(_) => Value::Void,
            complete_promise_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

impl From<run_entry_message::Result> for NonEmptyValue {
    fn from(value: run_entry_message::Result) -> Self {
        match value {
            run_entry_message::Result::Value(b) => NonEmptyValue::Success(b),
            run_entry_message::Result::Failure(f) => NonEmptyValue::Failure(f.into()),
        }
    }
}

impl TryFrom<get_call_invocation_id_entry_message::Result> for Value {
    type Error = Error;

    fn try_from(value: get_call_invocation_id_entry_message::Result) -> Result<Self, Self::Error> {
        Ok(match value {
            get_call_invocation_id_entry_message::Result::Value(id) => Value::InvocationId(id),
            get_call_invocation_id_entry_message::Result::Failure(f) => Value::Failure(f.into()),
        })
    }
}

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

// --- Completion parsing

#[derive(Debug)]
pub(crate) enum CompletionParsingHint {
    StateKeys,
    GetCompletionId,
    /// The normal case
    EmptyOrSuccessOrValue,
}

impl CompletionParsingHint {
    pub(crate) fn parse(self, result: completion_message::Result) -> Result<Value, Error> {
        match self {
            CompletionParsingHint::StateKeys => match result {
                completion_message::Result::Empty(_) => Err(EmptyStateKeys.into()),
                completion_message::Result::Value(b) => {
                    let mut state_keys = StateKeys::decode(b)
                        .map_err(DecodeStateKeysProst)?
                        .keys
                        .into_iter()
                        .map(|b| String::from_utf8(b.to_vec()).map_err(DecodeStateKeysUtf8))
                        .collect::<Result<Vec<_>, _>>()?;
                    state_keys.sort();

                    Ok(Value::StateKeys(state_keys))
                }
                completion_message::Result::Failure(f) => Ok(Value::Failure(f.into())),
            },
            CompletionParsingHint::GetCompletionId => match result {
                completion_message::Result::Empty(_) => Err(EmptyGetCallInvocationId.into()),
                completion_message::Result::Value(b) => Ok(Value::InvocationId(
                    String::from_utf8(b.to_vec()).map_err(DecodeGetCallInvocationIdUtf8)?,
                )),
                completion_message::Result::Failure(f) => Ok(Value::Failure(f.into())),
            },
            CompletionParsingHint::EmptyOrSuccessOrValue => Ok(match result {
                completion_message::Result::Empty(_) => Value::Void,
                completion_message::Result::Value(b) => Value::Success(b),
                completion_message::Result::Failure(f) => Value::Failure(f.into()),
            }),
        }
    }
}
