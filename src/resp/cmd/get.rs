#[derive(Debug)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub fn new<S>(key: S) -> Self
    where
        S: ToString,
    {
        Self {
            key: key.to_string(),
        }
    }
}
