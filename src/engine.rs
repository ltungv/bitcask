//! Define the interface for a storage engine and different implementations of that interface.

mod bitcask;
mod dashmapkv;
mod sledkv;

use std::str::FromStr;

pub use bitcask::{BitCaskConfig, BitCaskKeyValueStore};
pub use dashmapkv::DashMapKeyValueStore;
pub use sledkv::SledKeyValueStore;

/// A basic interface for a thread-safe key-value store that ensure consistent access to shared
/// data from multiple different threads.
pub trait KeyValueStore: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync;

    /// Set the value of a key, overwriting any existing value at that key and return the overwritten
    /// value
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Get the value of a key, if it exists. Return `None` if there's no value for the given key
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Delete a key and return its value, if it exists. Return `None` if the key does not exist
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;
}

/// Supported type of engine.
#[derive(Debug)]
pub enum Type {
    /// BitCask engine.
    BitCask,
    /// Sled database engine.
    Sled,
    /// In-memory engine.
    DashMap,
}

impl FromStr for Type {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "bitcask" => Ok(Self::BitCask),
            "sled" => Ok(Self::Sled),
            "dashmap" => Ok(Self::DashMap),
            _ => Err("unsupported"),
        }
    }
}
