//! Define the interface for a storage engine and different implementations of thatinterface

mod error;
mod inmem;

use bytes::Bytes;
pub use error::Error;
pub use inmem::InMemoryStorage;

/// Define the interface of a key-value store
pub trait KeyValueStore: Clone + Send + 'static {
    /// Sets a value to a key and returns the value previously associated with that key if it
    /// exists, otherwise, returns `None`.
    fn set(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, Error>;

    /// Returns the value of a key if the key exists, otherwise, returns `None`.
    fn get(&self, key: &str) -> Result<Option<Bytes>, Error>;

    /// Removes a key and returns the value associated with that key if its exists, otherwise,
    /// returns `None`.
    fn del(&self, key: &str) -> Result<Option<Bytes>, Error>;
}
