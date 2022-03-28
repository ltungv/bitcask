//! Implementations for the underlying storage engine

use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// A type alias for a our database type
pub struct StorageEngine {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl StorageEngine {
    /// Delete a key from the store. Returns the value of the removed
    /// key, if there's any
    pub fn del(&self, key: &str) -> Option<Bytes> {
        let mut map = self.inner.lock().unwrap();
        map.remove(key)
    }

    /// Get the value of a key from the store.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let map = self.inner.lock().unwrap();
        map.get(key).cloned()
    }

    /// Sets the value to a key. Returns the previous value of the key,
    /// if there's any.
    pub fn set(&self, key: &str, value: Bytes) -> Option<Bytes> {
        let mut map = self.inner.lock().unwrap();
        map.insert(key.to_string(), value)
    }
}

impl Clone for StorageEngine {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}
