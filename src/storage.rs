//! Define the interface for a storage engine and different implementations of that interface.

pub mod bitcask;
mod dashmapkv;
mod sledkv;

use bytes::Bytes;

pub use self::{dashmapkv::DashMapKeyValueStorage, sledkv::SledKeyValueStorage};

/// A basic interface for a thread-safe key-value store that ensure consistent access to shared
/// data from multiple different threads.
pub trait KeyValueStorage: Clone + Send + 'static {
    /// Error type of the underlying engine
    type Error: std::error::Error + Send + Sync;

    /// Set the value of a key and overwrite any existing value at that key.
    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;

    /// Get the value of a key, if it exists. Otherwise, return `None`.
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;

    /// Delete a key and return `true`, if it exists. Otherwise, return `false`.
    fn del(&self, key: Bytes) -> Result<bool, Self::Error>;
}
