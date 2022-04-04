use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use thiserror::Error;

use super::KeyValueStore;

/// A type alias for a our database type
#[derive(Default)]
pub struct DashMapKeyValueStore {
    inner: Arc<DashMap<Bytes, Bytes>>,
}

#[derive(Error, Debug)]
#[error("in-memory key-value store error")]
pub struct DashMapKeyValueStoreErrror;

impl KeyValueStore for DashMapKeyValueStore {
    type Error = DashMapKeyValueStoreErrror;

    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    fn del(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.inner.remove(key).map(|(_, v)| v))
    }

    /// Get the value of a key from the store.
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.inner.get(key).map(|e| e.value().clone()))
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    fn set(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.inner.insert(key, value))
    }
}

impl Clone for DashMapKeyValueStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
