use super::CommandParser;
use crate::{
    engine::KeyValueStore,
    error::{Error, ErrorKind},
    net::{Connection, Frame},
};
use tracing::debug;

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
        let key = parser
            .get_string()?
            .ok_or_else(|| Error::from(ErrorKind::InvalidFrame))?;
        if !parser.finish() {
            return Err(Error::from(ErrorKind::InvalidFrame));
        }
        Ok(Self { key })
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(self, storage: KV, connection: &mut Connection) -> Result<(), Error>
    where
        KV: KeyValueStore,
    {
        // Get the key's value
        // Responding with the received value
        let response = match storage.get(&self.key) {
            Ok(val) => Frame::BulkString(val),
            Err(e) if e.kind() == Some(ErrorKind::KeyNotFound) => Frame::Null,
            Err(e) => return Err(e),
        };
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
        Ok(())
    }
}
