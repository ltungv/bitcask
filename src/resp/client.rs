use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use super::{cmd::Get, Connection, Error, Frame};

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

    /// Get the value of the key.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Error> {
        let frame: Frame = Get::new(key).into();
        self.conn.write_frame(&frame).await?;
        debug!(request = ?frame);

        // Wait for the response from the server
        //
        // Both `Simple` and `Bulk` frames are accepted. `Null` represents the
        // key not being present and `None` is returned.
        match self.read_response().await? {
            Frame::BulkString(s) => Ok(Some(s)),
            Frame::Null => Ok(None),
            frame => Err(Error::UnexpectedFrame(frame)),
        }
    }

    async fn read_response(&mut self) -> Result<Frame, Error> {
        let frame = self.conn.read_frame().await?;
        debug!(response = ?frame);

        match frame {
            Some(Frame::Error(err)) => Err(Error::CommandFailed(err)),
            Some(frame) => Ok(frame),
            None => {
                // Server closes socket without sending data
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset by server",
                )
                .into())
            }
        }
    }
}
