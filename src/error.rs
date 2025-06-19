use crate::service_protocol::MessageType;
use std::borrow::Cow;
use std::time::Duration;
// Export some stuff we need from the internal package
pub use crate::vm::errors::{codes, InvocationErrorCode};

// -- Error type

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("State machine error [{code}]: {message}. Stacktrace: {stacktrace}")]
pub struct Error {
    pub(crate) code: u16,
    pub(crate) message: Cow<'static, str>,
    pub(crate) stacktrace: Cow<'static, str>,
    pub(crate) related_command: Option<RelatedCommand>,
    pub(crate) next_retry_delay: Option<Duration>,
}

impl Error {
    pub fn new(code: impl Into<u16>, message: impl Into<Cow<'static, str>>) -> Self {
        Error {
            code: code.into(),
            message: message.into(),
            stacktrace: Default::default(),
            related_command: None,
            next_retry_delay: None,
        }
    }

    pub fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(codes::INTERNAL, message)
    }

    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn description(&self) -> &str {
        &self.stacktrace
    }

    pub fn with_stacktrace(mut self, stacktrace: impl Into<Cow<'static, str>>) -> Self {
        self.stacktrace = stacktrace.into();
        self
    }

    pub fn with_related_command(mut self, related_command: RelatedCommand) -> Self {
        self.related_command = Some(related_command);
        self
    }

    pub fn with_next_retry_delay_override(mut self, delay: Duration) -> Self {
        self.next_retry_delay = Some(delay);
        self
    }

    /// Append the given description to the original one, in case the code is the same
    #[deprecated(note = "use `with_stacktrace` instead")]
    pub fn append_description_for_code(
        mut self,
        code: impl Into<u16>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let c = code.into();
        if self.code == c {
            if self.stacktrace.is_empty() {
                self.stacktrace = description.into();
            } else {
                self.stacktrace = format!("{}. {}", self.stacktrace, description.into()).into();
            }
            self
        } else {
            self
        }
    }

    pub fn is_suspended_error(&self) -> bool {
        self == &crate::vm::errors::SUSPENDED
    }
}

// -- Error relationships

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct RelatedCommand {
    pub(crate) name: Option<Cow<'static, str>>,
    pub(crate) index: Option<u32>,
    pub(crate) ty: Option<MessageType>,
}

impl RelatedCommand {
    pub(crate) fn new_named(
        name: impl Into<Cow<'static, str>>,
        index: u32,
        ty: MessageType,
    ) -> Self {
        Self {
            name: Some(name.into()),
            index: Some(index),
            ty: Some(ty),
        }
    }

    pub(crate) fn new(index: u32, ty: MessageType) -> Self {
        Self {
            name: None,
            index: Some(index),
            ty: Some(ty),
        }
    }
}
