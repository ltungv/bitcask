use super::CommandParser;
use crate::{
    net::{Connection, Error, Frame},
    storage::StorageEngine,
};
use tracing::debug;

/// Arguments for DEL command
#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    /// Creates a new set of arguments.
    ///
    /// DEL requires that the list of keys must have at least 1 element
    pub fn new<S>(keys: &[S]) -> Self
    where
        S: ToString,
    {
        Self {
            keys: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    /// Get the assigned keys
    pub fn keys(&self) -> std::slice::Iter<'_, String> {
        self.keys.iter()
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
