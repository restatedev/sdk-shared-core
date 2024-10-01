use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum Version {
    V1 = 1,
    V2 = 2,
}

const CONTENT_TYPE_V1: &str = "application/vnd.restate.invocation.v1";
const CONTENT_TYPE_V2: &str = "application/vnd.restate.invocation.v2";

impl Version {
    pub const fn content_type(&self) -> &'static str {
        match self {
            Version::V1 => CONTENT_TYPE_V1,
            Version::V2 => CONTENT_TYPE_V2,
        }
    }

    pub const fn minimum_supported_version() -> Self {
        Version::V2
    }

    pub const fn maximum_supported_version() -> Self {
        Version::V2
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.content_type())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported version '{0}'")]
pub struct UnsupportedVersionError(String);

impl FromStr for Version {
    type Err = UnsupportedVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            CONTENT_TYPE_V1 => Ok(Version::V1),
            CONTENT_TYPE_V2 => Ok(Version::V2),
            s => Err(UnsupportedVersionError(s.to_owned())),
        }
    }
}
