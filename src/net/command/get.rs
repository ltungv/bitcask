use tracing::debug;

use crate::{
    net::{self, connection::Connection, frame::Frame},
    storage::KeyValueStorage,
};

use super::Utf8Bytes;

/// Arguments for for GET command
#[derive(Debug, PartialEq, Eq)]
pub struct Get {
    key: Utf8Bytes,
}

impl Get {
    /// Creates a new set of arguments
    pub fn new(key: Utf8Bytes) -> Self {
        Self { key }
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(self, storage: KV, connection: &mut Connection) -> Result<(), net::Error>
    where
        KV: KeyValueStorage,
    {
        // Get the key's value
        let result = tokio::task::spawn_blocking(move || storage.get(self.key.as_ref().clone()))
            .await?
            .map_err(|e| net::Error::Storage(e.into()))?;

        // Responding with the received value
        let response = match result {
            Some(val) => Frame::BulkString(val),
            None => Frame::Null,
        };
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}

impl From<Get> for Frame {
    fn from(cmd: Get) -> Self {
        Self::Array(vec![
            Self::BulkString("GET".into()),
            Self::BulkString(cmd.key.as_ref().clone()),
        ])
    }
}
