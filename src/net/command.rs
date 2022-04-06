//! Implementations for a small set of commands as supported by Redis

mod del;
mod get;
mod set;

pub use del::Del;
pub use get::Get;
pub use set::Set;

use std::convert::TryFrom;

use bytes::Bytes;
use thiserror::Error;

use super::{connection::Connection, frame::Frame, shutdown::Shutdown};
use crate::storage::KeyValueStorage;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Key is not given")]
    NoKey,

    #[error("Value is not given")]
    NoValue,

    #[error("Invalid command encoding")]
    BadEncoding,

    #[error("Found an invalid command (got {0:?})")]
    BadCommand(String),

    #[error("Found an invalid frame (got {0:?})")]
    BadFrame(Frame),

    #[error("Could not parse bytes as an UTF-8 string - {0}")]
    NotUtf8(#[from] std::str::Utf8Error),
}

/// Enumeration of all the supported Redis commands. Each commands
/// will have an associated struct that contains its arguments' data
#[derive(Debug)]
pub enum Command {
    /// DEL key [key ...]
    Del(Del),
    /// GET key
    Get(Get),
    /// SET key value
    Set(Set),
}

impl Command {
    /// Applies the command to the underlying storage and sends back
    /// a response through the connection.
    ///
    /// Passing a `Shutdown` allows the function to finish its execution
    /// when the server is shutting down.
    pub async fn apply<KV>(
        self,
        storage: KV,
        connection: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), super::Error>
    where
        KV: KeyValueStorage,
    {
        match self {
            Command::Del(cmd) => cmd.apply(storage, connection).await,
            Command::Get(cmd) => cmd.apply(storage, connection).await,
            Command::Set(cmd) => cmd.apply(storage, connection).await,
        }
    }
}

impl TryFrom<Frame> for Command {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let mut parser = Parser::new(frame)?;
        match parser.get_bytes()? {
            Some(b) if "DEL" == b => Ok(Command::Del(parser.try_into()?)),
            Some(b) if "GET" == b => Ok(Command::Get(parser.try_into()?)),
            Some(b) if "SET" == b => Ok(Command::Set(parser.try_into()?)),
            Some(b) => Err(Error::BadCommand(String::from_utf8_lossy(&b).into())),
            None => Err(Error::BadCommand("".into())),
        }
    }
}

/// A parser that extracts values contained within a command frame
#[derive(Debug)]
pub struct Parser {
    frames: std::vec::IntoIter<Frame>,
}

impl Parser {
    pub fn new(frame: Frame) -> Result<Self, Error> {
        match frame {
            Frame::Array(frames) => Ok(Self {
                frames: frames.into_iter(),
            }),
            _ => Err(Error::BadFrame(frame)),
        }
    }

    /// Parses the next value in the frame as an UTF8 string.
    ///
    /// Returns a string if the next value can be represented as string.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    fn get_string(&mut self) -> Result<Option<Bytes>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => {
                std::str::from_utf8(&s[..])?;
                Ok(Some(s))
            }
            Some(f) => Err(Error::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as a bytes sequence.
    ///
    /// Returns a string if the next value can be represented as a bytes sequence.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    fn get_bytes(&mut self) -> Result<Option<Bytes>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(s)),
            Some(f) => Err(Error::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Ensure there are no more values
    fn finish(&mut self) -> bool {
        self.frames.next().is_none()
    }
}

impl TryFrom<Parser> for Del {
    type Error = Error;

    fn try_from(mut parser: Parser) -> Result<Self, Self::Error> {
        let mut keys = Vec::new();
        while let Some(key) = parser.get_string()? {
            keys.push(key)
        }
        if keys.is_empty() {
            return Err(Error::NoKey);
        }
        Ok(Self::new(keys))
    }
}

impl TryFrom<Parser> for Get {
    type Error = Error;

    fn try_from(mut parser: Parser) -> Result<Self, Self::Error> {
        let key = parser.get_string()?.ok_or(Error::NoKey)?;
        if !parser.finish() {
            return Err(Error::BadEncoding);
        }
        Ok(Self::new(key))
    }
}

impl TryFrom<Parser> for Set {
    type Error = Error;

    fn try_from(mut parser: Parser) -> Result<Self, Self::Error> {
        let key = parser.get_string()?.ok_or(Error::NoKey)?;
        let value = parser.get_bytes()?.ok_or(Error::NoValue)?;
        if !parser.finish() {
            return Err(Error::BadEncoding);
        }
        Ok(Self::new(key, value))
    }
}
