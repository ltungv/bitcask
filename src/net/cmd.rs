//! Implementations for a small set of commands as supported by Redis

mod del;
mod get;
mod set;

use crate::{
    net::{Connection, Error, Frame, Shutdown},
    engine::KeyValueStore,
};
use bytes::Bytes;
pub use del::Del;
pub use get::Get;
pub use set::Set;
use std::convert::TryFrom;

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
    pub async fn apply<KV: KeyValueStore>(
        self,
        storage: KV,
        connection: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), Error> {
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
        let mut parser = CommandParser::new(frame).ok_or(Error::InvalidFrame)?;
        match parser.get_bytes()? {
            Some(b) if "DEL" == b => Ok(Command::Del(Del::parse(parser)?)),
            Some(b) if "GET" == b => Ok(Command::Get(Get::parse(parser)?)),
            Some(b) if "SET" == b => Ok(Command::Set(Set::parse(parser)?)),
            Some(_) => Err(Error::UnexpectedFrame), // unknown or unsupported command
            _ => Err(Error::InvalidFrame),          // frame contains no data
        }
    }
}

/// A parser that extracts values contained within a command frame
#[derive(Debug)]
pub struct CommandParser {
    frames: std::vec::IntoIter<Frame>,
}

impl CommandParser {
    /// Creates a new parser from the given command frame.
    ///
    /// Returns the parser if the frame is of [`Frame::Array`] variant,
    /// Otherwise, returns `None`
    ///
    /// [`Frame::Array`]: super::Frame::Array
    pub fn new(frame: Frame) -> Option<Self> {
        if let Frame::Array(frames) = frame {
            Some(Self {
                frames: frames.into_iter(),
            })
        } else {
            None
        }
    }

    /// Parses the next value in the frame as an UTF8 string.
    ///
    /// Returns a string if the next value can be represented as string.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    pub fn get_string(&mut self) -> Result<Option<String>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(String::from_utf8(Vec::from(&s[..]))?)),
            Some(_) => Err(Error::UnexpectedFrame),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as a bytes sequence.
    ///
    /// Returns a string if the next value can be represented as a bytes sequence.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    pub fn get_bytes(&mut self) -> Result<Option<Bytes>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(s)),
            Some(_) => Err(Error::UnexpectedFrame),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as an 64-bit integer
    ///
    /// Returns an `i64` if the next value can be represented as an as an
    /// 64-bit integer.  Otherwise returns an error. Returns `None` if
    /// there's no value left.
    pub fn get_integer(&mut self) -> Result<Option<i64>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(atoi::atoi(&s[..]).ok_or(Error::InvalidFrame)?)),
            Some(_) => Err(Error::UnexpectedFrame),
            None => Ok(None),
        }
    }

    /// Ensure there are no more values
    pub fn finish(&mut self) -> bool {
        self.frames.next().is_none()
    }
}
