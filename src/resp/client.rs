use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use super::Connection;

/// Provide methods and hold states for manging a connection to a Redis server.
///
/// A connection can be established using the [`connect`] function. Once a connection is
/// established, requests to the server can be send using the corresponding methods of `Client`.
#[derive(Debug)]
pub struct Client {
    conn: Connection,
}

impl Client {
    /// Attempt to connect to the Redis server located at the given address.
    ///
    /// Returns a [`Client`] if a connection address exists and we can establish a connection
    /// with the address.
    ///
    /// [`Client`]: crate::resp::client::Client
    pub async fn connect<A>(addr: A) -> Result<Self, std::io::Error>
    where
        A: ToSocketAddrs,
    {
        let tcp = TcpStream::connect(addr).await?;
        let conn = Connection::new(tcp);
        Ok(Self { conn })
    }
    pub async fn set(&mut self, key: String, value: Bytes) -> Result<(), super::Error> {
        Ok(())
    }
}
