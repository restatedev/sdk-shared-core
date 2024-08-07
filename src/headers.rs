use std::convert::Infallible;
use std::fmt;

pub trait HeaderMap {
    type Error: fmt::Debug;

    fn extract(&self, name: &str) -> Result<Option<&str>, Self::Error>;
}

impl HeaderMap for Vec<(String, String)> {
    type Error = Infallible;

    fn extract(&self, name: &str) -> Result<Option<&str>, Self::Error> {
        for (k, v) in self {
            if k.eq_ignore_ascii_case(name) {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

#[cfg(feature = "http")]
impl HeaderMap for http::HeaderMap {
    type Error = http::header::ToStrError;

    fn extract(&self, name: &str) -> Result<Option<&str>, Self::Error> {
        self.get(name).map(|hv| hv.to_str()).transpose()
    }
}
