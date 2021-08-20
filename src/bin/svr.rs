use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use opal::resp::{Command, Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct HashMapStorageEngine {
    inner: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Clone for HashMapStorageEngine {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl HashMapStorageEngine {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn del(&self, key: &str) -> Option<Bytes> {
        let mut map = self.inner.lock().unwrap();
        map.remove(key)
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        let map = self.inner.lock().unwrap();
        map.get(key).cloned()
    }

    fn set(&mut self, key: &str, value: Bytes) -> Option<Bytes> {
        let mut map = self.inner.lock().unwrap();
        map.insert(key.to_string(), value)
    }
}

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let storage = HashMapStorageEngine::new();

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        let storage = storage.clone();
        tokio::spawn(async move {
            process(socket, storage).await;
        });
    }
}

async fn process(socket: TcpStream, mut storage: HashMapStorageEngine) {
    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::try_from(frame).unwrap() {
            Command::Set(cmd) => {
                storage.set(cmd.key(), cmd.value());
                Frame::SimpleString("OK".to_string())
            }
            Command::Get(cmd) => {
                if let Some(value) = storage.get(cmd.key()) {
                    Frame::BulkString(value.clone())
                } else {
                    Frame::Null
                }
            }
            Command::Del(cmd) => {
                let mut count = 0;
                for k in cmd.keys() {
                    if storage.del(k).is_some() {
                        count += 1;
                    }
                }
                Frame::Integer(count)
            }
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }

    if let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);

        // Respond with an error
        let response = Frame::Error("unimplemented".to_string());
        connection.write_frame(&response).await.unwrap();
    }
}
