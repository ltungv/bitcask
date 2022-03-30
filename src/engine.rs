//! Define the interface for a storage engine and different implementations of thatinterface

mod inmem;
mod lfs;
mod sledkv;

use crate::error::Error;
use bytes::Bytes;
pub use inmem::InMemoryStorage;
pub use lfs::LogStructuredHashTable;
pub use sledkv::SledKeyValueStore;
use std::str::FromStr;

/// Define the interface of a key-value store
pub trait KeyValueStore: Clone + Send + 'static {
    /// Sets a value to a key and returns the value previously associated with that key if it
    /// exists, otherwise, returns `None`.
    fn set(&self, key: String, value: Bytes) -> Result<(), Error>;

    /// Returns the value of a key if the key exists, otherwise, returns `None`.
    fn get(&self, key: &str) -> Result<Bytes, Error>;

    /// Removes a key and returns the value associated with that key if its exists, otherwise,
    /// returns `None`.
    fn del(&self, key: &str) -> Result<(), Error>;
}

/// Supported type of engine.
#[derive(Debug)]
pub enum Type {
    /// Log-structure file systems engine
    LFS,
    /// Sled database
    Sled,
    /// In-memory engine
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
