use std::sync::Arc;

use dashmap::DashMap;
use thiserror::Error;

use super::KeyValueStore;

/// A type alias for a our database type
#[derive(Default)]
pub struct DashMapKeyValueStore {
    inner: Arc<DashMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Error, Debug)]
#[error("in-memory key-value store error")]
pub struct DashMapKeyValueStoreErrror;

impl KeyValueStore for DashMapKeyValueStore {
    type Error = DashMapKeyValueStoreErrror;

    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    fn del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inner.remove(key).map(|(_, v)| v))
    }

    /// Get the value of a key from the store.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inner.get(key).map(|e| e.value().clone()))
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inner.insert(key.to_vec(), value.to_vec()))
    }
}

impl Clone for DashMapKeyValueStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
