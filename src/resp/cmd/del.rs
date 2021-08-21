use tracing::debug;

use crate::resp::{Connection, Error, Frame, StorageEngine};

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
            let mut count = 0;
            for k in &self.keys {
                if storage.del(k).is_some() {
                    count += 1;
                }
            }
            Frame::Integer(count)
        };

        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
        Ok(())
    }
}
