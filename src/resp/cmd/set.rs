use bytes::Bytes;

use crate::resp::Error;

use super::CommandParser;

/// Arguments for SET command
#[derive(Debug)]
pub struct Set {
    /// The key to set a value to
    pub key: String,
    /// The value to be set
    pub value: Bytes,
}

impl Set {
    /// Creates a new set of arguments
    pub fn new<S>(key: S, value: Bytes) -> Self
    where
        S: ToString,
    {
        Self {
            key: key.to_string(),
            value,
        }
    }

    /// Get the assigned key
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Get the assigned value
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    /// Get SET command arguments from the command parser
    pub fn parse(mut parser: CommandParser) -> Result<Self, Error> {
        let key = parser.get_string()?.ok_or(Error::InvalidFrame)?;
        let value = parser.get_bytes()?.ok_or(Error::InvalidFrame)?;

        if !parser.finish() {
            return Err(Error::InvalidFrame);
        }

        Ok(Self { key, value })
    }
}
