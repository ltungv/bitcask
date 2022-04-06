use anyhow::Result;
use bytes::Bytes;
use tracing::debug;

use crate::{
    net::{self, connection::Connection, frame::Frame},
    storage::KeyValueStorage,
};

/// Arguments for DEL command
#[derive(Debug)]
pub struct Del {
    keys: Vec<Bytes>,
}

impl Del {
    /// Creates a new set of arguments.
    ///
    /// DEL requires that the list of keys must have at least 1 element
    pub fn new(keys: Vec<Bytes>) -> Self {
        Self { keys }
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(self, storage: KV, connection: &mut Connection) -> Result<(), net::Error>
    where
        KV: KeyValueStorage,
    {
        // Delete the keys and count the number of deletions
        let count = tokio::task::spawn_blocking(move || {
            let mut count = 0;
            for key in &self.keys {
                match storage.del(key) {
                    Ok(Some(_)) => count += 1,
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                };
            }
            Ok(count)
        })
        .await?
        .map_err(|e: KV::Error| net::Error::Storage(e.into()))?;

        // Responding with the number of deletions
        let response = Frame::Integer(count);
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}

impl From<Del> for Frame {
    fn from(cmd: Del) -> Self {
        let mut cmd_data = vec![Self::BulkString("DEL".into())];
        for key in cmd.keys {
            cmd_data.push(Self::BulkString(key));
        }
        Self::Array(cmd_data)
    }
}
