//! Define the interface for a storage engine and different implementations of that interface.

mod inmem;
mod lfs;
mod sledkv;

use crate::error::Error;
use bytes::Bytes;
pub use inmem::InMemoryStorage;
pub use lfs::LogStructuredHashTable;
pub use sledkv::SledKeyValueStore;
use std::str::FromStr;

/// A basic interface for a thread-safe key-value store that ensure consistent access to shared
/// data from multiple different threads.
pub trait KeyValueStore: Clone + Send + 'static {
    /// Set the value of a key, overwriting any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn set(&self, key: String, value: Bytes) -> Result<(), Error>;

    /// Get the value of a key, if it exists.
    ///
    /// # Error
    ///
    /// If the queried key does not exist, returns an error of kind `ErrorKind::KeyNotFound`.
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn get(&self, key: &str) -> Result<Bytes, Error>;

    /// Delete a key and its value, if it exists.
    ///
    /// # Error
    ///
    /// If the deleted key does not exist, returns an error of kind `ErrorKind::KeyNotFound`.
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn del(&self, key: &str) -> Result<(), Error>;
}

/// Supported type of engine.
#[derive(Debug)]
pub enum Type {
    /// Log-structure file systems engine.
    LFS,
    /// Sled database engine.
    Sled,
    /// In-memory engine.
    InMem,
}

impl FromStr for Type {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "lfs" => Ok(Self::LFS),
            "sled" => Ok(Self::Sled),
            "memory" => Ok(Self::InMem),
            _ => Err("unsupported"),
        }
    }
}
