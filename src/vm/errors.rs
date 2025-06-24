use crate::fmt::DiffFormatter;
use crate::service_protocol::messages::{CommandMessageHeaderDiff, RestateMessage};
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

    pub const fn code(self) -> u16 {
        self.0
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
    pub const CLOSED: InvocationErrorCode = InvocationErrorCode(598);
    pub const SUSPENDED: InvocationErrorCode = InvocationErrorCode(599);
}

// Const errors

impl Error {
    const fn new_const(code: InvocationErrorCode, message: &'static str) -> Self {
        Error {
            code: code.0,
            message: Cow::Borrowed(message),
            stacktrace: Cow::Borrowed(""),
            related_command: None,
            next_retry_delay: None,
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
#[error("The execution replay ended unexpectedly. Expecting to read '{expected}' from the recorded journal, but the buffered messages were already drained.")]
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
    event: &'static str,
}

impl UnexpectedStateError {
    pub fn new(state: &'static str, event: &'static str) -> Self {
        Self { state, event }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("State machine was closed when invoking '{event:?}'")]
pub struct ClosedError {
    event: &'static str,
}

impl ClosedError {
    pub fn new(event: &'static str) -> Self {
        Self { event }
    }
}

#[derive(Debug)]
pub struct CommandTypeMismatchError {
    actual: MessageType,
    expected: MessageType,
}

impl CommandTypeMismatchError {
    pub fn new(actual: MessageType, expected: MessageType) -> CommandTypeMismatchError {
        Self { actual, expected }
    }
}

impl fmt::Display for CommandTypeMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
               "Found a mismatch between the code paths taken during the previous execution and the paths taken during this execution.
This typically happens when some parts of the code are non-deterministic.
 - The previous execution ran and recorded the following: '{}'
 - The current execution attempts to perform the following: '{}'",
               self.expected,
               self.actual,
        )
    }
}

impl std::error::Error for CommandTypeMismatchError {}

#[derive(Debug)]
pub struct CommandMismatchError<M> {
    command_index: i64,
    actual: M,
    expected: M,
}

impl<M> CommandMismatchError<M> {
    pub fn new(command_index: i64, actual: M, expected: M) -> CommandMismatchError<M> {
        Self {
            command_index,
            actual,
            expected,
        }
    }
}

impl<M: RestateMessage + CommandMessageHeaderDiff> fmt::Display for CommandMismatchError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
"Found a mismatch between the code paths taken during the previous execution and the paths taken during this execution.
This typically happens when some parts of the code are non-deterministic.
- The mismatch happened at index '{}' while executing '{}'
- Difference:",
            self.command_index,
            M::ty(),
        )?;
        self.actual
            .write_diff(&self.expected, DiffFormatter::new(f, "   "))
    }
}

impl<M: RestateMessage + CommandMessageHeaderDiff> std::error::Error for CommandMismatchError<M> {}

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
impl_error_code!(ClosedError, CLOSED);
impl_error_code!(CommandTypeMismatchError, JOURNAL_MISMATCH);
impl<M: RestateMessage + CommandMessageHeaderDiff> WithInvocationErrorCode
    for CommandMismatchError<M>
{
    fn code(&self) -> InvocationErrorCode {
        codes::JOURNAL_MISMATCH
    }
}
impl_error_code!(BadEagerStateKeyError, INTERNAL);
impl_error_code!(DecodeStateKeysProst, PROTOCOL_VIOLATION);
impl_error_code!(DecodeStateKeysUtf8, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerState, PROTOCOL_VIOLATION);
impl_error_code!(EmptyGetEagerStateKeys, PROTOCOL_VIOLATION);
impl_error_code!(UnsupportedFeatureForNegotiatedVersion, UNSUPPORTED_FEATURE);
