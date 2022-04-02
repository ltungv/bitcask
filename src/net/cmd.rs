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

use crate::{
    engine::KeyValueStore,
    net::{Connection, ConnectionError, Frame, Shutdown},
};

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("key not given")]
    NoKey,

    #[error("value not given")]
    NoValue,

    #[error("found unconsumed data")]
    Unconsumed,

    #[error("invalid integer string (got {0:?})")]
    NotInteger(Vec<u8>),

    #[error("invalid frame (got {0:?})")]
    BadFrame(Frame),

    #[error("invalid command (got {0:?})")]
    BadCommand(Vec<u8>),

    #[error(transparent)]
    NotUtf8(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Connection(#[from] ConnectionError),

    #[error("engine failed - {0}")]
    EngineError(#[source] anyhow::Error),
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
    ) -> Result<(), CommandError>
    where
        KV: KeyValueStore,
    {
        match self {
            Command::Del(cmd) => cmd.apply(storage, connection).await,
            Command::Get(cmd) => cmd.apply(storage, connection).await,
            Command::Set(cmd) => cmd.apply(storage, connection).await,
        }
    }
}

impl TryFrom<Frame> for Command {
    type Error = CommandError;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let mut parser = CommandParser::new(frame)?;
        match parser.get_bytes()? {
            Some(b) if "DEL" == b => Ok(Command::Del(Del::parse(parser)?)),
            Some(b) if "GET" == b => Ok(Command::Get(Get::parse(parser)?)),
            Some(b) if "SET" == b => Ok(Command::Set(Set::parse(parser)?)),
            Some(b) => Err(CommandError::BadCommand(b.to_vec())),
            None => Err(CommandError::BadCommand(vec![])),
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
    pub fn new(frame: Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Array(frames) => Ok(Self {
                frames: frames.into_iter(),
            }),
            _ => Err(CommandError::BadFrame(frame)),
        }
    }

    /// Parses the next value in the frame as an UTF8 string.
    ///
    /// Returns a string if the next value can be represented as string.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    pub fn get_string(&mut self) -> Result<Option<String>, CommandError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(String::from_utf8(Vec::from(&s[..]))?)),
            Some(f) => Err(CommandError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as a bytes sequence.
    ///
    /// Returns a string if the next value can be represented as a bytes sequence.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    pub fn get_bytes(&mut self) -> Result<Option<Bytes>, CommandError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(s)),
            Some(f) => Err(CommandError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as an 64-bit integer
    ///
    /// Returns an `i64` if the next value can be represented as an as an
    /// 64-bit integer.  Otherwise returns an error. Returns `None` if
    /// there's no value left.
    pub fn get_integer(&mut self) -> Result<Option<i64>, CommandError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(
                atoi::atoi(&s[..]).ok_or_else(|| CommandError::NotInteger(s.to_vec()))?,
            )),
            Some(f) => Err(CommandError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Ensure there are no more values
    pub fn finish(&mut self) -> bool {
        self.frames.next().is_none()
    }
}
