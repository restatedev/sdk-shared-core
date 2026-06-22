use std::fmt;
use std::sync::OnceLock;

pub trait ErrorFormatter: Send + Sync + fmt::Debug + 'static {
    fn display_closed_error(&self, f: &mut fmt::Formatter<'_>, event: &str) -> fmt::Result {
        write!(f, "State machine was closed when invoking '{event}'")
    }
}

static GLOBAL_ERROR_FORMATTER: OnceLock<Box<dyn ErrorFormatter>> = OnceLock::new();

/// Set the global error formatter.
///
/// The formatter can only be installed once: the first call wins and any
/// subsequent call is a no-op. Returns `true` if this call
/// installed the formatter, `false` if one was already set.
pub fn set_error_formatter(formatter: impl ErrorFormatter + 'static) -> bool {
    GLOBAL_ERROR_FORMATTER.set(Box::new(formatter)).is_ok()
}

#[derive(Debug)]
struct DefaultErrorFormatter;

impl ErrorFormatter for DefaultErrorFormatter {}

macro_rules! delegate_to_formatter {
    ($fn_name:ident($($param_name:ident: $param_type:ty),*) -> $return_type:ty) => {
        pub(crate) fn $fn_name($($param_name: $param_type),*) -> $return_type {
            if let Some(custom_formatter) = GLOBAL_ERROR_FORMATTER.get() {
                custom_formatter.$fn_name($($param_name),*)
            } else {
                DefaultErrorFormatter.$fn_name($($param_name),*)
            }
        }
    };
}

delegate_to_formatter!(display_closed_error(f: &mut fmt::Formatter<'_>, event: &str) -> fmt::Result);

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
