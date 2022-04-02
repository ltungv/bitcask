use super::{CommandError, CommandParser};
use crate::{
    engine::KeyValueStore,
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
    pub fn parse(mut parser: CommandParser) -> Result<Self, CommandError> {
        let key = parser.get_string()?.ok_or_else(|| CommandError::NoKey)?;
        let value = parser.get_bytes()?.ok_or_else(|| CommandError::NoValue)?;

        if !parser.finish() {
            return Err(CommandError::Unconsumed);
        }

        Ok(Self { key, value })
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
        // Set the key's value
        tokio::task::spawn_blocking(move || storage.set(self.key.as_bytes(), &self.value))
            .await?
            .map_err(|e| CommandError::EngineError(e.into()))?;

        // Responding OK
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}
