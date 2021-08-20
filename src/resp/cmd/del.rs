use crate::resp::Error;

use super::CommandParser;

/// Arguments for DEL command
#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    /// Creates a new set of arguments.
    ///
    /// DEL requires that the list of keys must have at least 1 element
    pub fn new<S>(key: S) -> Self
    where
        S: ToString,
    {
        Self {
            keys: vec![key.to_string()],
        }
    }

    /// Get the assigned keys
    pub fn keys(&self) -> std::slice::Iter<'_, String> {
        self.keys.iter()
    }

    /// Adds additional key for deletion.
    pub fn add_key<S>(&mut self, key: S)
    where
        S: ToString,
    {
        self.keys.push(key.to_string());
    }

    /// Get DEL command arguments from the command parser
    pub fn parse(mut parser: CommandParser) -> Result<Self, Error> {
        let mut keys = Vec::new();
        while let Some(key) = parser.get_string()? {
            keys.push(key);
        }

        if keys.is_empty() {
            return Err(Error::InvalidFrame);
        }
        Ok(Self { keys })
    }
}
