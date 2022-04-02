//! Implementations for a small set of commands as supported by Redis

pub mod parser;

use std::convert::TryFrom;

use bytes::Bytes;
use thiserror::Error;
use tracing::debug;

use super::{
    connection::{Connection, ConnectionError},
    frame::Frame,
    shutdown::Shutdown,
};
use crate::engine::KeyValueStore;
use parser::{CommandParseError, CommandParser};

#[derive(Error, Debug)]
pub enum CommandApplyError {
    #[error("Join error - {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Connection error - {0}")]
    Connection(#[from] ConnectionError),

    #[error("Engine failed - {0}")]
    KeyValueStore(#[source] anyhow::Error),
}

/// Enumeration of all the supported Redis commands. Each commands
/// will have an associated struct that contains its arguments' data
#[derive(Debug)]
pub enum Command {
    /// DEL key [key ...]
    Del(Del),
    /// GET key
    Get(Get),
    /// SET key value
    Set(Set),
}

impl Command {
    /// Applies the command to the underlying storage and sends back
    /// a response through the connection.
    ///
    /// Passing a `Shutdown` allows the function to finish its execution
    /// when the server is shutting down.
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), CommandApplyError>
    where
        KV: KeyValueStore,
    {
        match self {
            Command::Del(cmd) => cmd.apply(storage, connection).await,
            Command::Get(cmd) => cmd.apply(storage, connection).await,
            Command::Set(cmd) => cmd.apply(storage, connection).await,
        }
    }
}

impl TryFrom<Frame> for Command {
    type Error = CommandParseError;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        CommandParser::parse(frame)
    }
}

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

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
    ) -> Result<(), CommandApplyError>
    where
        KV: KeyValueStore,
    {
        // Delete the keys and count the number of deletions
        let count = tokio::task::spawn_blocking(move || {
            let mut count = 0;
            for key in &self.keys {
                match storage.del(key.as_bytes()) {
                    Ok(Some(_)) => count += 1,
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                };
            }
            Ok(count)
        })
        .await?
        .map_err(|e: KV::Error| CommandApplyError::KeyValueStore(e.into()))?;

        // Responding with the number of deletions
        let response = Frame::Integer(count);
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}

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

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
    ) -> Result<(), CommandApplyError>
    where
        KV: KeyValueStore,
    {
        // Get the key's value
        let result = tokio::task::spawn_blocking(move || storage.get(self.key.as_bytes()))
            .await?
            .map_err(|e| CommandApplyError::KeyValueStore(e.into()))?;

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

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Apply the command to the specified [`StorageEngine`] instance.
    ///
    /// [`StorageEngine`]: crate::StorageEngine;
    #[tracing::instrument(skip(self, storage, connection))]
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
    ) -> Result<(), CommandApplyError>
    where
        KV: KeyValueStore,
    {
        // Set the key's value
        tokio::task::spawn_blocking(move || storage.set(self.key.as_bytes(), &self.value))
            .await?
            .map_err(|e| CommandApplyError::KeyValueStore(e.into()))?;

        // Responding OK
        let response = Frame::SimpleString("OK".to_string());
        debug!(?response);

        // Write the response to the client
        connection.write_frame(&response).await?;
        Ok(())
    }
}
