//! Data structures and functions for parsing and representing values from RESP as
//! message frame a network environment

use std::io::Cursor;

use bytes::{Buf, Bytes};
use thiserror::Error;

use super::cmd::{Del, Get, Set};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum FrameError {
    #[error("Incomplete frame")]
    Incomplete,

    #[error("Invalid frame type byte (got {0})")]
    BadFrameTypeByte(u8),

    #[error("Invalid ending bytes (got {0} and {1})")]
    BadDelimiter(u8, u8),

    #[error("Invalid length (got {0})")]
    BadLength(i64),

    #[error("Invalid integer string (got {0:?})")]
    NotInteger(Vec<u8>),

    #[error("Invalid UTF-8 string - {0}")]
    NotUtf8(#[from] std::string::FromUtf8Error),
}

/// Data types as specified in [Redis Protocol (RESP)].
///
/// This is used as the smallest data unit that is accepted by the client
/// and the server when they communicate.
///
/// [Redis Protocol (RESP)]: https://redis.io/topics/protocol
#[derive(Debug, PartialEq)]
pub enum Frame {
    /// A simple string is an UTF8 encoded string that does not contain carriage-return
    /// nor line-feed
    SimpleString(String),
    /// An error is an UTF8 encoded string that does not contain carriage-return
    /// nor line-feed
    Error(String),
    /// An integer is a signed whole number whose value does not exceed 64 bits
    Integer(i64),
    /// A bulk string is a sequence of bytes
    BulkString(Bytes),
    /// An array is a sequence of frames
    Array(Vec<Frame>),
    /// A null does not carry meaning, represented by a special bulk-string value
    /// or a special array value
    Null,
}

impl Frame {
    /// Try to read data of a frame from the given reader.
    ///
    /// Returns the frame if it can be parsed from the reader, otherwise, returns an error.
    /// The error variant [`Error::Incomplete`] indicates that the reader does not have
    /// enough data for the frame; caller should retry later after receiving this error.
    ///
    /// [`Error::Incomplete`]: crate::resp::frame::Error::Incomplete
    pub fn parse(reader: &mut Cursor<&[u8]>) -> Result<Self, FrameError> {
        let frame = match get_byte(reader)? {
            b'+' => parse_simple_string(reader)?,
            b'-' => parse_error(reader)?,
            b':' => parse_integer(reader)?,
            b'$' => parse_bulk_string(reader)?,
            b'*' => parse_array(reader)?,
            b => return Err(FrameError::BadFrameTypeByte(b)),
        };
        Ok(frame)
    }

    /// Checks if a message frame can be parsed from the reader
    pub fn check(buf: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
        match get_byte(buf)? {
            b'+' => {
                get_line(buf)?;
            }
            b'-' => {
                get_line(buf)?;
            }
            b':' => {
                get_integer(buf)?;
            }
            b'$' => {
                let n = get_integer(buf)?;
                if n >= 0 {
                    let n: usize = n.try_into().map_err(|_| FrameError::BadLength(n))?;
                    // skip string length + 2 for "\r\n"
                    skip(buf, n + 2)?;
                }
            }
            b'*' => {
                let n = get_integer(buf)?;
                for _ in 0..n {
                    Frame::check(buf)?;
                }
            }
            b => return Err(FrameError::BadFrameTypeByte(b)),
        }
        Ok(())
    }
}

impl From<Del> for Frame {
    fn from(cmd: Del) -> Self {
        let mut cmd_data = vec![Self::BulkString("DEL".into())];
        for key in cmd.keys() {
            cmd_data.push(Self::BulkString(Bytes::copy_from_slice(key)));
        }
        Self::Array(cmd_data)
    }
}

impl From<Get> for Frame {
    fn from(cmd: Get) -> Self {
        Self::Array(vec![
            Self::BulkString("GET".into()),
            Self::BulkString(Bytes::copy_from_slice(cmd.key())),
        ])
    }
}

impl From<Set> for Frame {
    fn from(cmd: Set) -> Self {
        Self::Array(vec![
            Self::BulkString("SET".into()),
            Self::BulkString(Bytes::copy_from_slice(cmd.key())),
            Self::BulkString(Bytes::copy_from_slice(cmd.value())),
        ])
    }
}

fn parse_simple_string(reader: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    let line = get_line(reader)?;
    let simple_str = String::from_utf8(line.to_vec())?;
    Ok(Frame::SimpleString(simple_str))
}

fn parse_error(reader: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    let line = get_line(reader)?;
    let error_str = String::from_utf8(line.to_vec())?;
    Ok(Frame::Error(error_str))
}

fn parse_integer(reader: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    let int_value = get_integer(reader)?;
    Ok(Frame::Integer(int_value))
}

fn parse_bulk_string(reader: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    let bulk_len = get_integer(reader)?;
    if bulk_len == -1 {
        return Ok(Frame::Null);
    }

    let bulk_len = bulk_len
        .try_into()
        .map_err(|_| FrameError::BadLength(bulk_len))?;

    if (bulk_len + 2) > reader.remaining() {
        return Err(FrameError::Incomplete);
    }

    let bulk_bytes = reader.copy_to_bytes(bulk_len);
    skip(reader, 2)?;
    Ok(Frame::BulkString(bulk_bytes))
}

fn parse_array(reader: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    let array_len = get_integer(reader)?;
    if array_len == -1 {
        return Ok(Frame::Null);
    }

    let array_len = array_len
        .try_into()
        .map_err(|_| FrameError::BadLength(array_len))?;

    let mut items = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        items.push(Frame::parse(reader)?);
    }

    Ok(Frame::Array(items))
}

fn get_integer(buf: &mut Cursor<&[u8]>) -> Result<i64, FrameError> {
    let integer_str = get_line(buf)?;
    atoi::atoi(integer_str).ok_or_else(|| FrameError::NotInteger(integer_str.to_vec()))
}

fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], FrameError> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;
    for i in start..end {
        if buf.get_ref()[i] == b'\r' && buf.get_ref()[i + 1] == b'\n' {
            buf.set_position((i + 2) as u64);
            return Ok(&buf.get_ref()[start..i]);
        }
    }
    Err(FrameError::Incomplete)
}

fn get_byte(buf: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !buf.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    Ok(buf.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), FrameError> {
    if src.remaining() < n {
        return Err(FrameError::Incomplete);
    }
    src.advance(n);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn parse_simple_string_valid() {
        assert_frame(b"+OK\r\n", Frame::SimpleString("OK".to_string()));

        // extraneous '\r' and '\n' will not affect parsing
        assert_frame(b"+OK\r\r\n", Frame::SimpleString("OK\r".to_string()));
        assert_frame(b"+OK\n\r\n", Frame::SimpleString("OK\n".to_string()));
    }

    #[test]
    fn parse_error_valid() {
        assert_frame(b"-Error test\r\n", Frame::Error("Error test".to_string()));
        assert_frame(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        );
        assert_frame(
            b"-ERR unknown command 'foobar'\r\n",
            Frame::Error("ERR unknown command 'foobar'".to_string()),
        );

        // extraneous '\r' and '\n' will not affect parsing
        assert_frame(
            b"-Error test\r\r\n",
            Frame::Error("Error test\r".to_string()),
        );
        assert_frame(
            b"-Error test\n\r\n",
            Frame::Error("Error test\n".to_string()),
        );
    }

    #[test]
    fn parse_integer_valid() {
        assert_frame(b":1000\r\n", Frame::Integer(1000));
        assert_frame(b":-100\r\n", Frame::Integer(-100));
    }

    #[test]
    fn parse_integer_invalid_empty_buffer() {
        assert_frame_error(b":\r\n", FrameError::NotInteger(vec![]));
    }

    #[test]
    fn parse_integer_invalid_non_digit_character() {
        assert_frame_error(b":nan\r\n", FrameError::NotInteger(b"nan".to_vec()));
    }

    #[test]
    fn parse_integer_invalid_value_underflow() {
        assert_frame_error(
            b":-9223372036854775809\r\n",
            FrameError::NotInteger(b"-9223372036854775809".to_vec()),
        );
    }

    #[test]
    fn parse_integer_invalid_value_overflow() {
        assert_frame_error(
            b":9223372036854775808\r\n",
            FrameError::NotInteger(b"9223372036854775808".to_vec()),
        );
    }

    #[test]
    fn parse_bulk_string_valid() {
        assert_frame(b"$5\r\nhello\r\n", Frame::BulkString("hello".into()));
        assert_frame(b"$0\r\n\r\n", Frame::BulkString(Bytes::new()));

        // extraneous '\r' and '\n' will not affect parsing
        assert_frame(
            b"$11\r\nhello\rworld\r\n",
            Frame::BulkString("hello\nworld".into()),
        );
        assert_frame(
            b"$11\r\nhello\nworld\r\n",
            Frame::BulkString("hello\nworld".into()),
        );

        // parse bulk strings based on length prefix ignoring any sequence of "\r\n"
        assert_frame(
            b"$12\r\nhello\r\nworld\r\n",
            Frame::BulkString("hello\r\nworld".into()),
        );
        assert_frame(
            b"$6\r\nhello\r\nworld\r\n",
            Frame::BulkString("hello\r".into()),
        );
    }

    #[test]
    fn parse_bulk_string_invalid_length_prefix() {
        assert_frame_error(b"$-2\r\n", FrameError::BadLength(-2));
    }

    #[test]
    fn parse_bulk_string_invalid_length_prefix_too_large() {
        assert_frame_error(b"$24\r\nhello\r\nworld\r\n", FrameError::Incomplete);
    }

    #[test]
    fn parse_array_valid() {
        assert_frame(b"*0\r\n", Frame::Array(vec![]));

        assert_frame(
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            Frame::Array(vec![
                Frame::BulkString("foo".into()),
                Frame::BulkString("bar".into()),
            ]),
        );

        assert_frame(
            b"*3\r\n:1\r\n:2\r\n:3\r\n",
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
            ]),
        );

        assert_frame(
            b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
                Frame::Integer(4),
                Frame::BulkString("foobar".into()),
            ]),
        );

        assert_frame(
            b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::Integer(1),
                    Frame::Integer(2),
                    Frame::Integer(3),
                ]),
                Frame::Array(vec![
                    Frame::SimpleString("Foo".into()),
                    Frame::Error("Bar".into()),
                ]),
            ]),
        );

        assert_frame(
            b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
            Frame::Array(vec![
                Frame::BulkString("foo".into()),
                Frame::Null,
                Frame::BulkString("bar".into()),
            ]),
        );
    }

    #[test]
    fn parse_array_length_invalid_length_prefix() {
        assert_frame_error(b"*-2\r\n", FrameError::BadLength(-2));
    }

    #[test]
    fn parse_null_valid() {
        assert_frame(b"$-1\r\n", Frame::Null);
        assert_frame(b"*-1\r\n", Frame::Null);
    }

    fn assert_frame(input_data: &[u8], expected_frame: Frame) {
        let frame = parse_frame(input_data).unwrap();
        assert_eq!(frame, expected_frame);
    }

    fn assert_frame_error(input_data: &[u8], expected_err: FrameError) {
        let err = parse_frame(input_data).unwrap_err();
        assert_eq!(expected_err, err)
    }

    fn parse_frame(input_data: &[u8]) -> Result<Frame, FrameError> {
        let mut buf = Cursor::new(input_data);

        Frame::check(&mut buf)?;
        buf.set_position(0);

        Frame::parse(&mut buf)
    }
}
