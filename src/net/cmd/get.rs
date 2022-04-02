use bytes::Bytes;
use tracing::debug;

use super::{CommandError, CommandParser};
use crate::{
    engine::KeyValueStore,
    net::{connection::Connection, frame::Frame},
};

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
    pub fn parse(mut parser: CommandParser) -> Result<Self, CommandError> {
        let key = parser.get_string()?.ok_or(CommandError::NoKey)?;
        if !parser.finish() {
            return Err(CommandError::FoundUnconsumedData);
        }
        Ok(Self { key })
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
    ) -> Result<(), CommandError>
    where
        KV: KeyValueStore,
    {
        // Get the key's value
        let result = tokio::task::spawn_blocking(move || storage.get(self.key.as_bytes()))
            .await?
            .map_err(|e| CommandError::KeyValueStoreFailed(e.into()))?;

        // Responding with the received value
        let response = match result {
            Some(val) => Frame::BulkString(Bytes::from(val)),
            None => Frame::Null,
        };
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}
