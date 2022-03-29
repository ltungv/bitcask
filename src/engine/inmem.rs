use super::{Error, KeyValueStore};
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// A type alias for a our database type
#[derive(Default)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl KeyValueStore for InMemoryStorage {
    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    fn del(&self, key: &str) -> Result<Option<Bytes>, Error> {
        let mut map = self.inner.lock().unwrap();
        Ok(map.remove(key))
    }

    /// Get the value of a key from the store.
    fn get(&self, key: &str) -> Result<Option<Bytes>, Error> {
        let map = self.inner.lock().unwrap();
        Ok(map.get(key).cloned())
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    fn set(&self, key: &str, value: Bytes) -> Result<Option<Bytes>, Error> {
        let mut map = self.inner.lock().unwrap();
        Ok(map.insert(key.to_string(), value))
    }
}

impl Clone for InMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
