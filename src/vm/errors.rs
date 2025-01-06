use crate::service_protocol::{DecodingError, MessageType, UnsupportedVersionError};
use crate::{Error, Version};
use std::borrow::Cow;
use std::fmt;

// Error codes

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct InvocationErrorCode(u16);

impl InvocationErrorCode {
    pub const fn new(code: u16) -> Self {
        InvocationErrorCode(code)
    }
}

impl fmt::Debug for InvocationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for InvocationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<u16> for InvocationErrorCode {
    fn from(value: u16) -> Self {
        InvocationErrorCode(value)
    }
}

impl From<u32> for InvocationErrorCode {
    fn from(value: u32) -> Self {
        value
            .try_into()
            .map(InvocationErrorCode)
            .unwrap_or(codes::INTERNAL)
    }
}

impl From<InvocationErrorCode> for u16 {
    fn from(value: InvocationErrorCode) -> Self {
        value.0
    }
}

impl From<InvocationErrorCode> for u32 {
    fn from(value: InvocationErrorCode) -> Self {
        value.0 as u32
    }
}

pub mod codes {
    use super::InvocationErrorCode;

    pub const BAD_REQUEST: InvocationErrorCode = InvocationErrorCode(400);
    pub const INTERNAL: InvocationErrorCode = InvocationErrorCode(500);
    pub const UNSUPPORTED_MEDIA_TYPE: InvocationErrorCode = InvocationErrorCode(415);
    pub const JOURNAL_MISMATCH: InvocationErrorCode = InvocationErrorCode(570);
    pub const PROTOCOL_VIOLATION: InvocationErrorCode = InvocationErrorCode(571);
    pub const AWAITING_TWO_ASYNC_RESULTS: InvocationErrorCode = InvocationErrorCode(572);
    pub const UNSUPPORTED_FEATURE: InvocationErrorCode = InvocationErrorCode(573);
}

// Const errors

impl Error {
    const fn new_const(code: InvocationErrorCode, message: &'static str) -> Self {
        Error {
            code: code.0,
            message: Cow::Borrowed(message),
            description: Cow::Borrowed(""),
        }
    }
}

pub const MISSING_CONTENT_TYPE: Error = Error::new_const(
    codes::UNSUPPORTED_MEDIA_TYPE,
    "Missing content type when invoking",
);

pub const UNEXPECTED_INPUT_MESSAGE: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "Expected input message to be entry",
);

pub const KNOWN_ENTRIES_IS_ZERO: Error =
    Error::new_const(codes::INTERNAL, "Known entries is zero, expected >= 1");

pub const UNEXPECTED_ENTRY_MESSAGE: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "Expected entry messages only when waiting replay entries",
);

pub const INPUT_CLOSED_WHILE_WAITING_ENTRIES: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "The input was closed while still waiting to receive all the `known_entries`",
);

pub const EMPTY_IDEMPOTENCY_KEY: Error = Error::new_const(
    codes::INTERNAL,
    "Trying to execute an idempotent request with an empty idempotency key, this is not supported",
);

// Other errors

#[derive(Debug, Clone, thiserror::Error)]
#[error("Expecting entry {expected:?}, but the buffered entries were drained already. This is an invalid state")]
pub struct UnavailableEntryError {
    expected: MessageType,
}

impl UnavailableEntryError {
    pub fn new(expected: MessageType) -> Self {
        Self { expected }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unexpected state {state:?} when invoking '{event:?}'")]
pub struct UnexpectedStateError {
    state: &'static str,
    event: Box<dyn fmt::Debug + 'static>,
}

impl UnexpectedStateError {
    pub fn new(state: &'static str, event: impl fmt::Debug + 'static) -> Self {
        Self {
            state,
            event: Box::new(event),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Entry {actual:?} doesn't match expected entry {expected:?}")]
pub struct EntryMismatchError<M: fmt::Debug> {
    actual: M,
    expected: M,
}

impl<M: fmt::Debug> EntryMismatchError<M> {
    pub fn new(actual: M, expected: M) -> EntryMismatchError<M> {
        Self { actual, expected }
    }
}

impl<M: fmt::Debug> WithInvocationErrorCode for EntryMismatchError<M> {
    fn code(&self) -> InvocationErrorCode {
        codes::JOURNAL_MISMATCH
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Trying to await two async results at the same time: {previous} and {current}")]
pub struct AwaitingTwoAsyncResultError {
    pub(crate) previous: u32,
    pub(crate) current: u32,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Cannot convert a eager state key into UTF-8 String: {0:?}")]
pub struct BadEagerStateKeyError(#[from] pub(crate) std::string::FromUtf8Error);

#[derive(Debug, Clone, thiserror::Error)]
#[error("Cannot decode state keys message: {0}")]
pub struct DecodeStateKeysProst(#[from] pub(crate) prost::DecodeError);

#[derive(Debug, Clone, thiserror::Error)]
#[error("Cannot decode state keys message: {0}")]
pub struct DecodeStateKeysUtf8(#[from] pub(crate) std::string::FromUtf8Error);

#[derive(Debug, Clone, thiserror::Error)]
#[error("Unexpected empty value variant for get eager state")]
pub struct EmptyGetEagerState;

#[derive(Debug, Clone, thiserror::Error)]
#[error("Unexpected empty value variant for state keys")]
pub struct EmptyGetEagerStateKeys;

#[derive(Debug, thiserror::Error)]
#[error("Feature {feature} is not supported by the negotiated protocol version '{current_version}', the minimum required version is '{minimum_required_version}'")]
pub struct UnsupportedFeatureForNegotiatedVersion {
    feature: &'static str,
    current_version: Version,
    minimum_required_version: Version,
}

impl UnsupportedFeatureForNegotiatedVersion {
    pub fn new(
        feature: &'static str,
        current_version: Version,
        minimum_required_version: Version,
    ) -> Self {
        Self {
            feature,
            current_version,
            minimum_required_version,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Expecting entry to be either lazy or eager get state command, but was {actual:?}")]
pub struct UnexpectedGetState {
    pub(crate) actual: MessageType,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Expecting entry to be either lazy or eager get state keys command, but was {actual:?}")]
pub struct UnexpectedGetStateKeys {
    pub(crate) actual: MessageType,
}

// Conversions to VMError

trait WithInvocationErrorCode {
    fn code(&self) -> InvocationErrorCode;
}

impl<T: WithInvocationErrorCode + fmt::Display> From<T> for Error {
    fn from(value: T) -> Self {
        Error::new(value.code().0, value.to_string())
    }
}

macro_rules! impl_error_code {
    ($error_type:ident, $code:ident) => {
        impl WithInvocationErrorCode for $error_type {
            fn code(&self) -> InvocationErrorCode {
                codes::$code
            }
        }
    };
}

impl_error_code!(UnsupportedVersionError, UNSUPPORTED_MEDIA_TYPE);
impl WithInvocationErrorCode for DecodingError {
    fn code(&self) -> InvocationErrorCode {
        match self {
            DecodingError::UnexpectedMessageType { .. } => codes::JOURNAL_MISMATCH,
            _ => codes::INTERNAL,
        }
    }
}
impl_error_code!(UnavailableEntryError, JOURNAL_MISMATCH);
impl_error_code!(UnexpectedStateError, PROTOCOL_VIOLATION);
impl_error_code!(AwaitingTwoAsyncResultError, AWAITING_TWO_ASYNC_RESULTS);
impl_error_code!(BadEagerStateKeyError, INTERNAL);
impl_error_code!(DecodeStateKeysProst, PROTOCOL_VIOLATION);
impl_error_code!(DecodeStateKeysUtf8, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerState, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerStateKeys, PROTOCOL_VIOLATION);
impl_error_code!(UnsupportedFeatureForNegotiatedVersion, UNSUPPORTED_FEATURE);
impl_error_code!(UnexpectedGetState, JOURNAL_MISMATCH);
impl_error_code!(UnexpectedGetStateKeys, JOURNAL_MISMATCH);
