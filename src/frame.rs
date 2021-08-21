//! Data structures and functions for parsing and representing values from RESP as
//! message frame a network environment

use bytes::{Buf, Bytes};

use super::{
    cmd::{Del, Get, Set},
    Error,
};

const MAX_BULK_STRING_LENGTH: i64 = 512 * (1 << 20); // 512MB

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
    pub fn parse<R: Buf>(reader: &mut R) -> Result<Self, Error> {
        let frame = match get_byte(reader)? {
            b'+' => parse_simple_string(reader)?,
            b'-' => parse_error(reader)?,
            b':' => parse_integer(reader)?,
            b'$' => parse_bulk_string(reader)?,
            b'*' => parse_array(reader)?,
            _ => return Err(Error::InvalidFrame),
        };
        Ok(frame)
    }

    /// Checks if a message frame can be parsed from the reader
    pub fn check<R: Buf>(reader: &mut R) -> Result<(), Error> {
        match get_byte(reader)? {
            b'+' => {
                let n = get_line_length(reader)?;
                reader.advance(n);
            }
            b'-' => {
                let n = get_line_length(reader)?;
                reader.advance(n);
            }
            b':' => {
                get_integer(reader)?;
            }
            b'$' => {
                let n = get_integer(reader)?;
                if n >= 0 {
                    let data_len = n as usize + 2; // skip data + '\r\n'
                    if data_len > reader.remaining() {
                        return Err(Error::IncompleteFrame);
                    }
                    reader.advance(data_len);
                }
            }
            b'*' => {
                let n = get_integer(reader)?;
                for _ in 0..n {
                    Frame::check(reader)?;
                }
            }
            _ => return Err(Error::InvalidFrame),
        }
        Ok(())
    }
}

impl From<Del> for Frame {
    fn from(cmd: Del) -> Self {
        let mut cmd_data = vec![Self::BulkString("DEL".into())];
        for key in cmd.keys() {
            cmd_data.push(Self::BulkString(key.to_owned().into()));
        }
        Self::Array(cmd_data)
    }
}

impl From<Get> for Frame {
    fn from(cmd: Get) -> Self {
        Self::Array(vec![
            Self::BulkString("GET".into()),
            Self::BulkString(cmd.key().to_owned().into()),
        ])
    }
}

impl From<Set> for Frame {
    fn from(cmd: Set) -> Self {
        Self::Array(vec![
            Self::BulkString("SET".into()),
            Self::BulkString(cmd.key.into()),
            Self::BulkString(cmd.value),
        ])
    }
}

fn parse_simple_string<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let line_length = get_line_length(reader)?;
    let simple_str = reader.copy_to_bytes(line_length - 2);
    reader.advance(2); // "\r\n"

    let simple_str = String::from_utf8(simple_str.to_vec())?;
    Ok(Frame::SimpleString(simple_str))
}

fn parse_error<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let line_length = get_line_length(reader)?;
    let error_str = reader.copy_to_bytes(line_length - 2);
    reader.advance(2); // "\r\n"

    let error_str = String::from_utf8(error_str.to_vec())?;
    Ok(Frame::Error(error_str))
}

fn parse_integer<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let int_value = get_integer(reader)?;
    Ok(Frame::Integer(int_value))
}

fn parse_bulk_string<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let bulk_len = get_integer(reader)?;
    if bulk_len == -1 {
        return Ok(Frame::Null);
    }
    if !(0..=MAX_BULK_STRING_LENGTH).contains(&bulk_len) {
        return Err(Error::InvalidFrame);
    }

    let bulk_len = bulk_len as usize;
    if (bulk_len + 2) > reader.remaining() {
        return Err(Error::IncompleteFrame);
    }
    if reader.chunk()[bulk_len] != b'\r' || reader.chunk()[bulk_len + 1] != b'\n' {
        return Err(Error::InvalidFrame);
    }

    let bulk_bytes = reader.copy_to_bytes(bulk_len as usize);
    reader.advance(2);
    Ok(Frame::BulkString(bulk_bytes))
}

fn parse_array<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let array_len = get_integer(reader)?;
    if array_len == -1 {
        return Ok(Frame::Null);
    }
    if array_len < 0 {
        return Err(Error::InvalidFrame);
    }

    let array_len = array_len as usize;
    let mut items = Vec::with_capacity(array_len);
    let mut items_remain = array_len;

    while items_remain > 0 {
        items.push(Frame::parse(reader)?);
        items_remain -= 1;
    }

    Ok(Frame::Array(items))
}

fn get_integer<R: Buf>(reader: &mut R) -> Result<i64, Error> {
    let line_length = get_line_length(reader)?;
    let integer_str = reader.copy_to_bytes(line_length - 2);
    reader.advance(2); // "\r\n"
    atoi::atoi(&integer_str[..]).ok_or(Error::InvalidFrame)
}

fn get_line_length<R: Buf>(reader: &mut R) -> Result<usize, Error> {
    let reader_chunk = reader.chunk();
    let reader_bytes = reader.remaining();

    for i in 0..reader_bytes - 1 {
        match reader_chunk[i] {
            b'\r' => {
                if reader_chunk[i + 1] == b'\n' {
                    return Ok(i + 2);
                }
                return Err(Error::InvalidFrame);
            }
            b'\n' => {
                return Err(Error::InvalidFrame);
            }
            _ => {}
        }
    }
    Err(Error::IncompleteFrame)
}

fn get_byte<R: Buf>(reader: &mut R) -> Result<u8, Error> {
    if !reader.has_remaining() {
        return Err(Error::IncompleteFrame);
    }
    Ok(reader.get_u8())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn parse_simple_string_valid() {
        assert_frame(b"+OK\r\n", Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_simple_string_invalid_carriage_return() {
        assert_frame_error(b"+OK\r\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_simple_string_invalid_line_feed() {
        assert_frame_error(b"+OK\n\r\n", Error::InvalidFrame);
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
    }

    #[test]
    fn parse_error_invalid_carriage_return() {
        assert_frame_error(b"-Error test\r\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_error_invalid_line_feed() {
        assert_frame_error(b"-Error test\n\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_integer_valid() {
        assert_frame(b":1000\r\n", Frame::Integer(1000));
    }

    #[test]
    fn parse_integer_invalid_empty_buffer() {
        assert_frame_error(b":\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_integer_invalid_non_digit_character() {
        assert_frame_error(b":nan\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_integer_invalid_value_underflow() {
        assert_frame_error(b":-9223372036854775809\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_integer_invalid_value_overflow() {
        assert_frame_error(b":9223372036854775808\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_bulk_string_valid() {
        assert_frame(b"$5\r\nhello\r\n", Frame::BulkString("hello".into()));
        assert_frame(b"$0\r\n\r\n", Frame::BulkString(Bytes::new()));
        assert_frame(
            b"$11\r\nhello\nworld\r\n",
            Frame::BulkString("hello\nworld".into()),
        );
        assert_frame(
            b"$12\r\nhello\r\nworld\r\n",
            Frame::BulkString("hello\r\nworld".into()),
        );
    }

    #[test]
    fn parse_bulk_string_invalid_length_prefix() {
        assert_frame_error(b"$-2\r\n", Error::InvalidFrame);
    }

    #[test]
    fn parse_bulk_string_invalid_length_prefix_too_large() {
        assert_frame_error(b"$24\r\nhello\r\nworld\r\n", Error::IncompleteFrame);
    }

    #[test]
    fn parse_bulk_string_invalid_length_prefix_too_small() {
        assert_frame_error(b"$6\r\nhello\r\nworld\r\n", Error::InvalidFrame);
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
        assert_frame_error(b"*-2\r\n", Error::InvalidFrame);
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

    fn assert_frame_error(input_data: &[u8], expected_err: Error) {
        let err = parse_frame(input_data).unwrap_err();
        let assertion = match (err, expected_err) {
            (Error::IncompleteFrame, Error::IncompleteFrame) => true,
            (Error::InvalidFrame, Error::InvalidFrame) => true,
            (Error::FromUtf8Error(e1), Error::FromUtf8Error(e2)) => e1 == e2,
            _ => unimplemented!("Unreleated errors"),
        };
        assert!(assertion);
    }

    fn parse_frame(input_data: &[u8]) -> Result<Frame, Error> {
        let mut buf = Cursor::new(input_data);

        Frame::check(&mut buf)?;
        buf.set_position(0);

        Frame::parse(&mut buf)
    }
}