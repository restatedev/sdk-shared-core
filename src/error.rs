use crate::service_protocol::MessageType;
use crate::CommandType;
use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

// Export some stuff we need from the internal package
pub use crate::vm::errors::{codes, InvocationErrorCode};

// -- Error type

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct CommandMetadata {
    pub(crate) index: u32,
    pub(crate) ty: MessageType,
    pub(crate) name: Option<Cow<'static, str>>,
}

impl fmt::Display for CommandMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.ty)?;
        if let Some(name) = &self.name {
            write!(f, "[{}]", name)?;
        } else {
            write!(f, "[{}]", self.index)?;
        }
        Ok(())
    }
}

impl CommandMetadata {
    pub(crate) fn new_named(
        name: impl Into<Cow<'static, str>>,
        index: u32,
        ty: MessageType,
    ) -> Self {
        Self {
            name: Some(name.into()),
            index,
            ty,
        }
    }

    #[allow(unused)]
    pub(crate) fn new(index: u32, ty: MessageType) -> Self {
        Self {
            name: None,
            index,
            ty,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Error {
    pub(crate) code: u16,
    pub(crate) message: Cow<'static, str>,
    pub(crate) stacktrace: Cow<'static, str>,
    pub(crate) related_command: Option<CommandMetadata>,
    pub(crate) next_retry_delay: Option<Duration>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}) {}", self.code, self.message)?;
        if !self.stacktrace.is_empty() {
            write!(f, "\nStacktrace: {}", self.stacktrace)?;
        }
        if let Some(related_command) = &self.related_command {
            write!(f, "\nRelated command: {}", related_command)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {}

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

    pub(crate) fn with_related_command_metadata(
        mut self,
        related_command: CommandMetadata,
    ) -> Self {
        self.related_command = Some(related_command);
        self
    }
}

impl From<CommandType> for MessageType {
    fn from(value: CommandType) -> Self {
        match value {
            CommandType::Input => MessageType::InputCommand,
            CommandType::Output => MessageType::OutputCommand,
            CommandType::GetState => MessageType::GetLazyStateCommand,
            CommandType::GetStateKeys => MessageType::GetLazyStateKeysCommand,
            CommandType::SetState => MessageType::SetStateCommand,
            CommandType::ClearState => MessageType::ClearStateCommand,
            CommandType::ClearAllState => MessageType::ClearAllStateCommand,
            CommandType::GetPromise => MessageType::GetPromiseCommand,
            CommandType::PeekPromise => MessageType::PeekPromiseCommand,
            CommandType::CompletePromise => MessageType::CompletePromiseCommand,
            CommandType::Sleep => MessageType::SleepCommand,
            CommandType::Call => MessageType::CallCommand,
            CommandType::OneWayCall => MessageType::OneWayCallCommand,
            CommandType::SendSignal => MessageType::SendSignalCommand,
            CommandType::Run => MessageType::RunCommand,
            CommandType::AttachInvocation => MessageType::AttachInvocationCommand,
            CommandType::GetInvocationOutput => MessageType::GetInvocationOutputCommand,
            CommandType::CompleteAwakeable => MessageType::CompleteAwakeableCommand,
            CommandType::CancelInvocation => MessageType::SendSignalCommand,
        }
    }
}

impl TryFrom<MessageType> for CommandType {
    type Error = MessageType;

    fn try_from(value: MessageType) -> Result<Self, Self::Error> {
        match value {
            MessageType::InputCommand => Ok(CommandType::Input),
            MessageType::OutputCommand => Ok(CommandType::Output),
            MessageType::GetLazyStateCommand | MessageType::GetEagerStateCommand => {
                Ok(CommandType::GetState)
            }
            MessageType::GetLazyStateKeysCommand | MessageType::GetEagerStateKeysCommand => {
                Ok(CommandType::GetStateKeys)
            }
            MessageType::SetStateCommand => Ok(CommandType::SetState),
            MessageType::ClearStateCommand => Ok(CommandType::ClearState),
            MessageType::ClearAllStateCommand => Ok(CommandType::ClearAllState),
            MessageType::GetPromiseCommand => Ok(CommandType::GetPromise),
            MessageType::PeekPromiseCommand => Ok(CommandType::PeekPromise),
            MessageType::CompletePromiseCommand => Ok(CommandType::CompletePromise),
            MessageType::SleepCommand => Ok(CommandType::Sleep),
            MessageType::CallCommand => Ok(CommandType::Call),
            MessageType::OneWayCallCommand => Ok(CommandType::OneWayCall),
            MessageType::SendSignalCommand => Ok(CommandType::SendSignal),
            MessageType::RunCommand => Ok(CommandType::Run),
            MessageType::AttachInvocationCommand => Ok(CommandType::AttachInvocation),
            MessageType::GetInvocationOutputCommand => Ok(CommandType::GetInvocationOutput),
            MessageType::CompleteAwakeableCommand => Ok(CommandType::CompleteAwakeable),
            _ => Err(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_type_to_command_type_conversion() {
        // Test successful conversions
        assert_eq!(
            CommandType::try_from(MessageType::InputCommand).unwrap(),
            CommandType::Input
        );
        assert_eq!(
            CommandType::try_from(MessageType::OutputCommand).unwrap(),
            CommandType::Output
        );
        assert_eq!(
            CommandType::try_from(MessageType::GetLazyStateCommand).unwrap(),
            CommandType::GetState
        );
        assert_eq!(
            CommandType::try_from(MessageType::GetLazyStateKeysCommand).unwrap(),
            CommandType::GetStateKeys
        );
        assert_eq!(
            CommandType::try_from(MessageType::SetStateCommand).unwrap(),
            CommandType::SetState
        );
        assert_eq!(
            CommandType::try_from(MessageType::ClearStateCommand).unwrap(),
            CommandType::ClearState
        );
        assert_eq!(
            CommandType::try_from(MessageType::ClearAllStateCommand).unwrap(),
            CommandType::ClearAllState
        );
        assert_eq!(
            CommandType::try_from(MessageType::GetPromiseCommand).unwrap(),
            CommandType::GetPromise
        );
        assert_eq!(
            CommandType::try_from(MessageType::PeekPromiseCommand).unwrap(),
            CommandType::PeekPromise
        );
        assert_eq!(
            CommandType::try_from(MessageType::CompletePromiseCommand).unwrap(),
            CommandType::CompletePromise
        );
        assert_eq!(
            CommandType::try_from(MessageType::SleepCommand).unwrap(),
            CommandType::Sleep
        );
        assert_eq!(
            CommandType::try_from(MessageType::CallCommand).unwrap(),
            CommandType::Call
        );
        assert_eq!(
            CommandType::try_from(MessageType::OneWayCallCommand).unwrap(),
            CommandType::OneWayCall
        );
        assert_eq!(
            CommandType::try_from(MessageType::SendSignalCommand).unwrap(),
            CommandType::SendSignal
        );
        assert_eq!(
            CommandType::try_from(MessageType::RunCommand).unwrap(),
            CommandType::Run
        );
        assert_eq!(
            CommandType::try_from(MessageType::AttachInvocationCommand).unwrap(),
            CommandType::AttachInvocation
        );
        assert_eq!(
            CommandType::try_from(MessageType::GetInvocationOutputCommand).unwrap(),
            CommandType::GetInvocationOutput
        );
        assert_eq!(
            CommandType::try_from(MessageType::CompleteAwakeableCommand).unwrap(),
            CommandType::CompleteAwakeable
        );

        // Test failed conversions
        assert_eq!(
            CommandType::try_from(MessageType::Start).err().unwrap(),
            MessageType::Start
        );
        assert_eq!(
            CommandType::try_from(MessageType::End).err().unwrap(),
            MessageType::End
        );
        assert_eq!(
            CommandType::try_from(MessageType::GetLazyStateCompletionNotification)
                .err()
                .unwrap(),
            MessageType::GetLazyStateCompletionNotification
        );
    }
}
