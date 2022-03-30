use crate::{
    engine::KeyValueStore,
    error::{Error, ErrorKind},
};
use bytes::Bytes;

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
    fn set(&self, key: String, value: Bytes) -> Result<(), Error> {
        self.db
            .insert(key, value.to_vec())
            .map_err(|e| Error::new(ErrorKind::CommandFailed, e))?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Bytes, Error> {
        let val = self
            .db
            .get(key)
            .map_err(|e| Error::new(ErrorKind::CommandFailed, e))?
            .ok_or_else(|| Error::from(ErrorKind::KeyNotFound))?;
        Ok(Bytes::copy_from_slice(val.as_ref()))
    }

    fn del(&self, key: &str) -> Result<(), Error> {
        self.db
            .remove(key)
            .map_err(|e| Error::new(ErrorKind::CommandFailed, e))?
            .ok_or_else(|| Error::from(ErrorKind::KeyNotFound))?;
        Ok(())
    }
}
