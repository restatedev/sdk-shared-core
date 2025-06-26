use crate::CommandType;
use std::fmt;
use std::sync::OnceLock;

pub trait CommandFormatter: Send + Sync + fmt::Debug + 'static {
    fn ty_display(&self, ty: CommandType) -> &'static str;
}

static GLOBAL_COMMAND_FORMATTER: OnceLock<Box<dyn CommandFormatter>> = OnceLock::new();

// Public function for the crate user to set the formatter
pub fn set_error_formatter(formatter: impl CommandFormatter + 'static) {
    GLOBAL_COMMAND_FORMATTER
        .set(Box::new(formatter))
        .expect("Error formatter already set! It can only be set once.");
}

#[derive(Debug)]
struct DefaultCommandFormatter;

impl CommandFormatter for DefaultCommandFormatter {
    fn ty_display(&self, ty: CommandType) -> &'static str {
        match ty {
            CommandType::Input => "handler input",
            CommandType::Output => "handler return",
            CommandType::GetState => "get state",
            CommandType::GetStateKeys => "get state keys",
            CommandType::SetState => "set state",
            CommandType::ClearState => "clear state",
            CommandType::ClearAllState => "clear all state",
            CommandType::GetPromise => "get promise",
            CommandType::PeekPromise => "peek promise",
            CommandType::CompletePromise => "complete promise",
            CommandType::Sleep => "sleep",
            CommandType::Call => "call",
            CommandType::OneWayCall => "one way call/send",
            CommandType::SendSignal => "send signal",
            CommandType::Run => "run",
            CommandType::AttachInvocation => "attach invocation",
            CommandType::GetInvocationOutput => "get invocation output",
            CommandType::CompleteAwakeable => "complete awakeable",
            CommandType::CancelInvocation => "cancel invocation",
        }
    }
}

pub(crate) fn display_command_ty(command_type: CommandType) -> &'static str {
    if let Some(custom_formatter) = GLOBAL_COMMAND_FORMATTER.get() {
        custom_formatter.ty_display(command_type)
    } else {
        DefaultCommandFormatter.ty_display(command_type)
    }
}

pub(crate) struct DiffFormatter<'a, 'b> {
    fmt: &'a mut fmt::Formatter<'b>,
    indentation: &'static str,
}

impl<'a, 'b: 'a> DiffFormatter<'a, 'b> {
    pub(crate) fn new(fmt: &'a mut fmt::Formatter<'b>, indentation: &'static str) -> Self {
        Self { fmt, indentation }
    }

    pub(crate) fn write_diff(
        &mut self,
        field_name: &'static str,
        actual: impl fmt::Display,
        expected: impl fmt::Display,
    ) -> fmt::Result {
        write!(
            self.fmt,
            "\n{}{field_name}: {actual} != {expected}",
            self.indentation
        )
    }

    pub(crate) fn write_bytes_diff(
        &mut self,
        field_name: &'static str,
        actual: &[u8],
        expected: &[u8],
    ) -> fmt::Result {
        write!(self.fmt, "\n{}{field_name}: ", self.indentation)?;
        match (std::str::from_utf8(actual), std::str::from_utf8(expected)) {
            (Ok(actual), Ok(expected)) => {
                write!(self.fmt, "'{actual}' != '{expected}'",)
            }
            (Ok(actual), Err(_)) => {
                write!(self.fmt, "'{actual}' != {expected:?}")
            }
            (Err(_), Ok(expected)) => {
                write!(self.fmt, "{actual:?} != '{expected}'")
            }
            (Err(_), Err(_)) => {
                write!(self.fmt, "{actual:?} != {expected:?}")
            }
        }
    }
}
