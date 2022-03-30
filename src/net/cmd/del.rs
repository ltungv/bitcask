use super::CommandParser;
use crate::{
    engine::KeyValueStore,
    error::{Error, ErrorKind},
    net::{Connection, Frame},
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
            return Err(Error::from(ErrorKind::InvalidFrame));
        }
        Ok(Self { keys })
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(self, storage: KV, connection: &mut Connection) -> Result<(), Error>
    where
        KV: KeyValueStore,
    {
        // Delete the keys and count the number of deletions
        let mut count = 0;
        for k in &self.keys {
            match storage.del(k) {
                Ok(_) => count += 1,
                Err(e) if e.kind() == Some(ErrorKind::KeyNotFound) => continue,
                Err(e) => return Err(e),
            }
        }

        // Responding with the number of deletions
        let response = Frame::Integer(count);
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
        Ok(())
    }
}
