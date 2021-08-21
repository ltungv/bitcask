use tracing::debug;

use crate::resp::{Connection, Error, Frame, StorageEngine};

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

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply(
        self,
        storage: &StorageEngine,
        connection: &mut Connection,
    ) -> Result<(), Error> {
        // Set the key's value
        let response = {
            if let Some(value) = storage.get(&self.key) {
                // Returns the value as a bulk string
                Frame::BulkString(value)
            } else {
                // Key not found
                Frame::Null
            }
        };

        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
        Ok(())
    }
}
