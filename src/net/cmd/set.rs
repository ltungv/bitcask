use bytes::Bytes;
use tracing::debug;

use crate::{
    net::{self, connection::Connection, frame::Frame},
    storage::KeyValueStorage,
};

/// Arguments for SET command
#[derive(Debug)]
pub struct Set {
    /// The key to set a value to
    key: Bytes,
    /// The value to be set
    value: Bytes,
}

impl Set {
    /// Creates a new set of arguments
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(self, storage: KV, connection: &mut Connection) -> Result<(), net::Error>
    where
        KV: KeyValueStorage,
    {
        // Set the key's value
        tokio::task::spawn_blocking(move || storage.set(self.key, self.value))
            .await?
            .map_err(|e| net::Error::Storage(e.into()))?;

        // Responding OK
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}

impl From<Set> for Frame {
    fn from(cmd: Set) -> Self {
        Self::Array(vec![
            Self::BulkString("SET".into()),
            Self::BulkString(cmd.key),
            Self::BulkString(cmd.value),
        ])
    }
}
