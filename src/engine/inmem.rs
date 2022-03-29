use super::KeyValueStore;
use crate::error::{Error, ErrorKind};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// A type alias for a our database type
#[derive(Default)]
pub struct InMemoryStorage {
    inner: Arc<DashMap<String, Bytes>>,
}

impl KeyValueStore for InMemoryStorage {
    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    fn del(&self, key: &str) -> Result<(), Error> {
        match self.inner.remove(key) {
            Some(_) => Ok(()),
            None => Err(Error::from(ErrorKind::KeyNotFound)),
        }
    }

    /// Get the value of a key from the store.
    fn get(&self, key: &str) -> Result<Bytes, Error> {
        match self.inner.get(key) {
            Some(v) => Ok(v.clone()),
            None => Err(Error::from(ErrorKind::KeyNotFound)),
        }
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    fn set(&self, key: String, value: Bytes) -> Result<(), Error> {
        self.inner.insert(key, value);
        Ok(())
    }
}

impl Clone for InMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
