use crate::service_protocol::{ContentTypeError, DecodingError, MessageType};
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
    pub const SUSPENDED: InvocationErrorCode = InvocationErrorCode(599);
}

// Const errors

impl Error {
    const fn new_const(code: InvocationErrorCode, message: &'static str) -> Self {
        Error {
            code: code.0,
            message: Cow::Borrowed(message),
            stacktrace: Cow::Borrowed(""),
        }
    }
}

pub const MISSING_CONTENT_TYPE: Error = Error::new_const(
    codes::UNSUPPORTED_MEDIA_TYPE,
    "Missing content type when invoking the service deployment",
);

pub const UNEXPECTED_INPUT_MESSAGE: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "Expected incoming message to be an entry",
);

pub const KNOWN_ENTRIES_IS_ZERO: Error =
    Error::new_const(codes::INTERNAL, "Known entries is zero, expected >= 1");

pub const UNEXPECTED_ENTRY_MESSAGE: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "Expected entry messages only when waiting replay entries",
);

pub const INPUT_CLOSED_WHILE_WAITING_ENTRIES: Error = Error::new_const(
    codes::PROTOCOL_VIOLATION,
    "The input was closed while still waiting to receive all journal to replay",
);

pub const EMPTY_IDEMPOTENCY_KEY: Error = Error::new_const(
    codes::INTERNAL,
    "Trying to execute an idempotent request with an empty idempotency key. The idempotency key must be non-empty.",
);

pub const SUSPENDED: Error = Error::new_const(codes::SUSPENDED, "Suspended invocation");

// Other errors

#[derive(Debug, Clone, thiserror::Error)]
#[error("The journal replay unexpectedly ended. Expecting to read entry {expected:?} from the replayed journal, but the buffered entries were drained already.")]
pub struct UnavailableEntryError {
    expected: MessageType,
}

impl UnavailableEntryError {
    pub fn new(expected: MessageType) -> Self {
        Self { expected }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Unexpected state '{state:?}' when invoking '{event:?}'")]
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
#[error("Replayed journal doesn't match the handler code at index {command_index}.\nThe handler code generated: {expected:#?}\nwhile the replayed entry is: {actual:#?}")]
pub struct CommandMismatchError<M: fmt::Debug> {
    command_index: i64,
    actual: M,
    expected: M,
}

impl<M: fmt::Debug> CommandMismatchError<M> {
    pub fn new(command_index: i64, actual: M, expected: M) -> CommandMismatchError<M> {
        Self {
            command_index,
            actual,
            expected,
        }
    }
}

impl<M: fmt::Debug> WithInvocationErrorCode for CommandMismatchError<M> {
    fn code(&self) -> InvocationErrorCode {
        codes::JOURNAL_MISMATCH
    }
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
#[error("Feature '{feature}' is not supported by the negotiated protocol version '{current_version}', the minimum required version is '{minimum_required_version}'")]
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
#[error("Replayed journal doesn't match the handler code at index {command_index}.\nThe handler code generated a 'get state command',\nwhile the replayed entry is: {actual:#?}")]
pub struct UnexpectedGetState {
    pub(crate) command_index: i64,
    pub(crate) actual: MessageType,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Replayed journal doesn't match the handler code at index {command_index}.\nThe handler code generated a 'get state keys command',\nwhile the replayed entry is: {actual:#?}")]
pub struct UnexpectedGetStateKeys {
    pub(crate) command_index: i64,
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

impl_error_code!(ContentTypeError, UNSUPPORTED_MEDIA_TYPE);
impl WithInvocationErrorCode for DecodingError {
    fn code(&self) -> InvocationErrorCode {
        match self {
            DecodingError::UnexpectedMessageType { .. } => codes::JOURNAL_MISMATCH,
            _ => codes::INTERNAL,
        }
    }
}
impl_error_code!(UnavailableEntryError, PROTOCOL_VIOLATION);
impl_error_code!(UnexpectedStateError, PROTOCOL_VIOLATION);
impl_error_code!(BadEagerStateKeyError, INTERNAL);
impl_error_code!(DecodeStateKeysProst, PROTOCOL_VIOLATION);
impl_error_code!(DecodeStateKeysUtf8, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerState, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerStateKeys, PROTOCOL_VIOLATION);
impl_error_code!(UnsupportedFeatureForNegotiatedVersion, UNSUPPORTED_FEATURE);
impl_error_code!(UnexpectedGetState, JOURNAL_MISMATCH);
impl_error_code!(UnexpectedGetStateKeys, JOURNAL_MISMATCH);
