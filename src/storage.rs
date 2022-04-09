//! Define the interface for a storage engine and different implementations of that interface.

mod bitcask;
mod dashmapkv;
mod sledkv;

use bytes::Bytes;

pub use self::{
    bitcask::{Config, Handle},
    dashmapkv::DashMapKeyValueStorage,
    sledkv::SledKeyValueStorage,
};

/// A basic interface for a thread-safe key-value store that ensure consistent access to shared
/// data from multiple different threads.
pub trait KeyValueStorage: Clone + Send + 'static {
    /// Error type of the underlying engine
    type Error: std::error::Error + Send + Sync;

    /// Set the value of a key, overwriting any existing value at that key and return the overwritten
    /// value
    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;

    /// Get the value of a key, if it exists. Return `None` if there's no value for the given key
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;

    /// Delete a key and return its value, if it exists. Return `None` if the key does not exist
    fn del(&self, key: Bytes) -> Result<bool, Self::Error>;
}
