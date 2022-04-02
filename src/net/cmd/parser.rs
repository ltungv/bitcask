use anyhow::Result;
use bytes::Bytes;
use thiserror::Error;

use crate::net::frame::Frame;

use super::{Command, Del, Get, Set};

#[derive(Error, Debug)]
pub enum CommandParseError {
    #[error("Key not given")]
    NoKey,

    #[error("Value not given")]
    NoValue,

    #[error("Found unconsumed data")]
    FoundUnconsumedData,

    #[error("Invalid command (got {0:?})")]
    BadCommand(Vec<u8>),

    #[error("Invalid frame (got {0:?})")]
    BadFrame(Frame),

    #[error("Invalid integer string (got {0:?})")]
    NotInteger(Vec<u8>),

    #[error("Invalid UTF-8 string - {0}")]
    NotUtf8(#[from] std::string::FromUtf8Error),
}

/// A parser that extracts values contained within a command frame
#[derive(Debug)]
pub struct CommandParser {
    frames: std::vec::IntoIter<Frame>,
}

impl CommandParser {
    pub fn parse(frame: Frame) -> Result<Command, CommandParseError> {
        match frame {
            Frame::Array(frames) => {
                let mut parser = Self {
                    frames: frames.into_iter(),
                };
                parser.parse_cmd()
            }
            _ => Err(CommandParseError::BadFrame(frame)),
        }
    }

    fn parse_cmd(&mut self) -> Result<Command, CommandParseError> {
        match self.get_bytes()? {
            Some(b) if "DEL" == b => Ok(Command::Del(self.parse_del()?)),
            Some(b) if "GET" == b => Ok(Command::Get(self.parse_get()?)),
            Some(b) if "SET" == b => Ok(Command::Set(self.parse_set()?)),
            Some(b) => Err(CommandParseError::BadCommand(b.to_vec())),
            None => Err(CommandParseError::BadCommand(vec![])),
        }
    }

    fn parse_del(&mut self) -> Result<Del, CommandParseError> {
        let mut keys = Vec::new();
        while let Some(key) = self.get_string()? {
            keys.push(key);
        }
        if keys.is_empty() {
            return Err(CommandParseError::NoKey);
        }
        Ok(Del { keys })
    }

    fn parse_get(&mut self) -> Result<Get, CommandParseError> {
        let key = self.get_string()?.ok_or(CommandParseError::NoKey)?;
        if !self.finish() {
            return Err(CommandParseError::FoundUnconsumedData);
        }
        Ok(Get { key })
    }

    fn parse_set(&mut self) -> Result<Set, CommandParseError> {
        let key = self.get_string()?.ok_or(CommandParseError::NoKey)?;
        let value = self.get_bytes()?.ok_or(CommandParseError::NoValue)?;
        if !self.finish() {
            return Err(CommandParseError::FoundUnconsumedData);
        }
        Ok(Set { key, value })
    }

    /// Parses the next value in the frame as an UTF8 string.
    ///
    /// Returns a string if the next value can be represented as string.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    fn get_string(&mut self) -> Result<Option<String>, CommandParseError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(String::from_utf8(Vec::from(&s[..]))?)),
            Some(f) => Err(CommandParseError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as a bytes sequence.
    ///
    /// Returns a string if the next value can be represented as a bytes sequence.
    /// Otherwise returns an error. Returns `None` if there's no value left.
    fn get_bytes(&mut self) -> Result<Option<Bytes>, CommandParseError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => Ok(Some(s)),
            Some(f) => Err(CommandParseError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Parses the next value in the frame as an 64-bit integer
    ///
    /// Returns an `i64` if the next value can be represented as an as an
    /// 64-bit integer.  Otherwise returns an error. Returns `None` if
    /// there's no value left.
    fn get_integer(&mut self) -> Result<Option<i64>, CommandParseError> {
        match self.frames.next() {
            Some(Frame::BulkString(s)) => {
                Ok(Some(atoi::atoi(&s[..]).ok_or_else(|| {
                    CommandParseError::NotInteger(s.to_vec())
                })?))
            }
            Some(f) => Err(CommandParseError::BadFrame(f)),
            None => Ok(None),
        }
    }

    /// Ensure there are no more values
    fn finish(&mut self) -> bool {
        self.frames.next().is_none()
    }
}
