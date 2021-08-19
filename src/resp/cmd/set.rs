use bytes::Bytes;

#[derive(Debug)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl Set {
    pub fn new<S>(key: S, value: Bytes) -> Self
    where
        S: ToString,
    {
        Self {
            key: key.to_string(),
            value,
        }
    }
}
