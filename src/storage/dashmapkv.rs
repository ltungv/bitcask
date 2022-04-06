use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use thiserror::Error;

use super::KeyValueStorage;

/// A type alias for a our database type
#[derive(Default)]
pub struct DashMapKeyValueStorage {
    inner: Arc<DashMap<Bytes, Bytes>>,
}

#[derive(Error, Debug)]
#[error("In-memory key-value store error")]
pub struct DashMapKeyValueStoreErrror;

impl KeyValueStorage for DashMapKeyValueStorage {
    type Error = DashMapKeyValueStoreErrror;

    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    fn del(&self, key: Bytes) -> Result<bool, Self::Error> {
        Ok(self.inner.remove(&key).is_some())
    }

    /// Get the value of a key from the store.
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error> {
        Ok(self.inner.get(&key).map(|e| e.value().clone()))
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        self.inner.insert(key, value);
        Ok(())
    }
}

impl Clone for DashMapKeyValueStorage {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
