use crate::service_protocol::ContentTypeError;
use std::fmt;
use std::str::FromStr;

/// A version of the explicit recovery protocol. Its numbers are its own: they are never values
/// of [`crate::Version`], the durable execution protocol's.
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum ProtocolVersion {
    V1 = 1,
}

const CONTENT_TYPE_PREFIX: &str = "application/vnd.restate.invocation.explicitrecovery.";
const CONTENT_TYPE_V1: &str = "application/vnd.restate.invocation.explicitrecovery.v1";

impl ProtocolVersion {
    pub const fn content_type(&self) -> &'static str {
        match self {
            ProtocolVersion::V1 => CONTENT_TYPE_V1,
        }
    }

    pub const fn minimum_supported_version() -> Self {
        ProtocolVersion::V1
    }

    pub const fn maximum_supported_version() -> Self {
        ProtocolVersion::V1
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.content_type())
    }
}

impl FromStr for ProtocolVersion {
    type Err = ContentTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            CONTENT_TYPE_V1 => Ok(ProtocolVersion::V1),
            // A version of this family this build does not speak, or another family: both a 415.
            s if s.starts_with(CONTENT_TYPE_PREFIX) => {
                Err(ContentTypeError::RestateContentType(s.to_owned()))
            }
            s if s.starts_with("application/vnd.restate.invocation.") => {
                Err(ContentTypeError::RestateContentType(s.to_owned()))
            }
            s => Err(ContentTypeError::OtherContentType(s.to_owned())),
        }
    }
}
