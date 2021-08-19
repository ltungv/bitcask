#[derive(Debug)]
pub struct Del {
    pub keys: Vec<String>,
}

impl Del {
    pub fn new<S>(key: S) -> Self
    where
        S: ToString,
    {
        Self {
            keys: vec![key.to_string()],
        }
    }

    pub fn add_key<S>(&mut self, key: S)
    where
        S: ToString,
    {
        self.keys.push(key.to_string());
    }
}
