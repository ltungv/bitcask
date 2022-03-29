use super::{
    cmd::{Del, Get, Set},
    Connection, Frame,
};
use crate::error::{Error, ErrorKind};
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

/// Provide methods and hold states for manging a connection to a Redis server.
///
/// A connection can be established using the [`connect`] function. Once a connection is
/// established, requests to the server can be send using the corresponding methods of `Client`.
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
        match self.read_response().await? {
            Frame::BulkString(s) => Ok(Some(s)), // retrieved key's value
            Frame::Null => Ok(None),             // key does not exist
            _ => Err(ErrorKind::UnexpectedFrame.into()),
        }
    }

    /// Set the value of the key, overwritting the value that is currently held by
    /// the key, regardless of its type.
    ///
    /// The SET command supports a set of options that modify its behavior:
    /// - (unsupported) EX seconds -- Set the specified expire time, in seconds.
    /// - (unsupported) PX milliseconds -- Set the specified expire time, in milliseconds.
    /// - (unsupported) EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    /// - (unsupported) PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
    /// - (unsupported) NX -- Only set the key if it does not already exist.
    /// - (unsupported) XX -- Only set the key if it already exist.
    /// - (unsupported) KEEPTTL -- Retain the time to live associated with the key.
    /// - (unsupported) GET -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
    #[tracing::instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<(), Error> {
        self.set_cmd(Set::new(key, value)).await
    }

    async fn set_cmd(&mut self, cmd: Set) -> Result<(), Error> {
        let frame: Frame = cmd.into();
        self.conn.write_frame(&frame).await?;

        debug!(request = ?frame);

        // Wait for the response from the server
        match self.read_response().await? {
            Frame::SimpleString(s) if s == "OK" => Ok(()), // suceeded
            _ => Err(ErrorKind::UnexpectedFrame.into()),   // error occured / unsupported reply
        }
    }

    /// Removes the specified keys, ignoring non-existed keys.
    ///
    /// Returns the number of keys that were removed.
    #[tracing::instrument(skip(self))]
    pub async fn del(&mut self, keys: &[String]) -> Result<i64, Error> {
        if keys.is_empty() {
            return Err(Error::new(
                ErrorKind::CommandFailed,
                "must specify at least 1 key".to_string(),
            ));
        }

        // already checked for non-empty slice with the if-condition
        let cmd = Del::new(keys);

        let frame: Frame = cmd.into();
        self.conn.write_frame(&frame).await?;
        debug!(request = ?frame);

        // Wait for the response from the server
        match self.read_response().await? {
            Frame::Integer(n) => Ok(n),
            _ => Err(ErrorKind::UnexpectedFrame.into()),
        }
    }

    async fn read_response(&mut self) -> Result<Frame, Error> {
        let frame = self.conn.read_frame().await?;
        debug!(response = ?frame);

        match frame {
            Some(Frame::Error(err)) => Err(Error::new(ErrorKind::CommandFailed, err)),
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
