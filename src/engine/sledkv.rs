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

    fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db.insert(key, value).map(|v| v.map(|v| v.to_vec()))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db.get(key).map(|v| v.map(|v| v.to_vec()))
    }

    fn del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db.remove(key).map(|v| v.map(|v| v.to_vec()))
    }
}
