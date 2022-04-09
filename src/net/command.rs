//! Implementations for a small set of commands as supported by Redis

mod del;
mod get;
mod set;

use std::convert::TryFrom;

use bytes::Bytes;
use thiserror::Error;

pub use self::{del::Del, get::Get, set::Set};
use super::{connection::Connection, frame::Frame};
use crate::{shutdown::Shutdown, storage::KeyValueStorage};

/// Error from parsing command from frame
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    /// Command arguments are invalid
    #[error("Invalid command arguments - {0}")]
    BadArguments(&'static str),

    /// Encountered an invalid command
    #[error("Invalid command (got {0:?})")]
    BadCommand(String),

    /// Encountered an unexpected frame
    #[error("Invalid frame (got {0:?})")]
    BadFrame(Frame),

    /// Could not parse utf8 string
    #[error("Could not parse bytes as an UTF-8 string - {0}")]
    NotUtf8(#[from] std::string::FromUtf8Error),
}

/// Enumeration of all the supported Redis commands. Each commands
/// will have an associated struct that contains its arguments' data
#[derive(Debug, PartialEq, Eq)]
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
    /// Create a new parse for the given frame
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
    fn get_string(&mut self) -> Result<Option<String>, Error> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => {
                let s = String::from_utf8(s.to_vec())?;
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
            return Err(Error::BadArguments("Keys are empty"));
        }
        Ok(Self::new(keys))
    }
}

impl TryFrom<Parser> for Get {
    type Error = Error;

    fn try_from(mut parser: Parser) -> Result<Self, Self::Error> {
        let key = parser
            .get_string()?
            .ok_or(Error::BadArguments("Key is not given"))?;
        if !parser.finish() {
            return Err(Error::BadArguments("Frame contains extra data"));
        }
        Ok(Self::new(key))
    }
}

impl TryFrom<Parser> for Set {
    type Error = Error;

    fn try_from(mut parser: Parser) -> Result<Self, Self::Error> {
        let key = parser
            .get_string()?
            .ok_or(Error::BadArguments("Key is not given"))?;
        let value = parser
            .get_bytes()?
            .ok_or(Error::BadArguments("Value is not given"))?;
        if !parser.finish() {
            return Err(Error::BadArguments("Frame contains extra data"));
        }
        Ok(Self::new(key, value))
    }
}

#[cfg(test)]
mod tests {
    use crate::net::frame::Frame;

    use super::*;

    #[test]
    fn parse_get_ok() {
        assert_command(
            Frame::Array(vec![
                Frame::BulkString("GET".into()),
                Frame::BulkString("hello".into()),
            ]),
            Command::Get(Get::new("hello".into())),
        )
    }

    #[test]
    fn parse_get_no_key() {
        assert_error(
            Frame::Array(vec![Frame::BulkString("GET".into())]),
            Error::BadArguments("Key is not given"),
        )
    }

    #[test]
    fn parse_get_extra_keys() {
        assert_error(
            Frame::Array(vec![
                Frame::BulkString("GET".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("hello".into()),
            ]),
            Error::BadArguments("Frame contains extra data"),
        )
    }

    #[test]
    fn parse_set_ok() {
        assert_command(
            Frame::Array(vec![
                Frame::BulkString("SET".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("world".into()),
            ]),
            Command::Set(Set::new("hello".into(), "world".into())),
        )
    }

    #[test]
    fn parse_set_no_key() {
        assert_error(
            Frame::Array(vec![Frame::BulkString("SET".into())]),
            Error::BadArguments("Key is not given"),
        )
    }

    #[test]
    fn parse_set_no_value() {
        assert_error(
            Frame::Array(vec![
                Frame::BulkString("SET".into()),
                Frame::BulkString("hello".into()),
            ]),
            Error::BadArguments("Value is not given"),
        )
    }

    #[test]
    fn parse_set_extra_data() {
        assert_error(
            Frame::Array(vec![
                Frame::BulkString("SET".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("hello".into()),
                Frame::BulkString("hello".into()),
            ]),
            Error::BadArguments("Frame contains extra data"),
        )
    }

    #[test]
    fn parse_del_ok() {
        assert_command(
            Frame::Array(vec![
                Frame::BulkString("DEL".into()),
                Frame::BulkString("hello".into()),
            ]),
            Command::Del(Del::new(vec!["hello".into()])),
        );
        assert_command(
            Frame::Array(vec![
                Frame::BulkString("DEL".into()),
                Frame::BulkString("hello1".into()),
                Frame::BulkString("hello2".into()),
                Frame::BulkString("hello3".into()),
                Frame::BulkString("hello4".into()),
                Frame::BulkString("hello5".into()),
            ]),
            Command::Del(Del::new(vec![
                "hello1".into(),
                "hello2".into(),
                "hello3".into(),
                "hello4".into(),
                "hello5".into(),
            ])),
        );
    }

    #[test]
    fn parse_del_no_key() {
        assert_error(
            Frame::Array(vec![Frame::BulkString("DEL".into())]),
            Error::BadArguments("Keys are empty"),
        )
    }

    #[test]
    fn parse_invalid_command() {
        assert_error(
            Frame::Array(vec![Frame::BulkString("INVALID".into())]),
            Error::BadCommand("INVALID".into()),
        );
    }

    fn assert_command(frame: Frame, cmd: Command) {
        let parsed = Command::try_from(frame).unwrap();
        assert_eq!(parsed, cmd);
    }

    fn assert_error(frame: Frame, err: Error) {
        let parsed_err = Command::try_from(frame).unwrap_err();
        assert_eq!(parsed_err, err);
    }
}
