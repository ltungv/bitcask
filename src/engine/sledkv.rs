use bytes::Bytes;
use sled::IVec;

use crate::engine::KeyValueStore;

/// A key-value store that uses sled as the underlying data storage engine
#[derive(Debug, Clone)]
pub struct SledKeyValueStore {
    db: sled::Db,
}

impl SledKeyValueStore {
    /// Creates a new proxy that forwards method calls to the underlying key-value store
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }
}

impl KeyValueStore for SledKeyValueStore {
    type Error = sled::Error;

    fn set(&self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.db
            .insert(IVec::from(key.as_ref()), IVec::from(value.as_ref()))
            .map(|v| v.map(|v| Bytes::copy_from_slice(v.as_ref())))
    }

    fn get(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.db
            .get(key)
            .map(|v| v.map(|v| Bytes::copy_from_slice(v.as_ref())))
    }

    fn del(&self, key: &Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.db
            .remove(key)
            .map(|v| v.map(|v| Bytes::copy_from_slice(v.as_ref())))
    }
}
