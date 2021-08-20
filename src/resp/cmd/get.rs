use crate::resp::Error;

use super::CommandParser;

/// Arguments for for GET command
#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    /// Creates a new set of arguments
    pub fn new<S>(key: S) -> Self
    where
        S: ToString,
    {
        Self {
            key: key.to_string(),
        }
    }

    /// Get the assigned key
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Get GET command arguments from the command parser
    pub fn parse(mut parser: CommandParser) -> Result<Self, Error> {
        let key = parser.get_string()?.ok_or(Error::InvalidFrame)?;
        if !parser.finish() {
            return Err(Error::InvalidFrame);
        }
        Ok(Self { key })
    }
}
