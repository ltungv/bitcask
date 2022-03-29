use super::CommandParser;
use crate::{
    engine::KeyValueStore,
    error::{Error, ErrorKind},
    net::{Connection, Frame},
};
use bytes::Bytes;
use tracing::debug;

/// Arguments for SET command
#[derive(Debug)]
pub struct Set {
    /// The key to set a value to
    key: String,
    /// The value to be set
    value: Bytes,
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
        let key = parser
            .get_string()?
            .ok_or_else(|| Error::from(ErrorKind::InvalidFrame))?;
        let value = parser
            .get_bytes()?
            .ok_or_else(|| Error::from(ErrorKind::InvalidFrame))?;

        if !parser.finish() {
            return Err(Error::from(ErrorKind::InvalidFrame));
        }
        Ok(Self { key, value })
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV: KeyValueStore>(
        self,
        storage: KV,
        connection: &mut Connection,
    ) -> Result<(), Error> {
        // Set the key's value
        storage.set(self.key, self.value)?;

        // Responding OK
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
        Ok(())
    }
}
