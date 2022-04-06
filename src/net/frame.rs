//! Data structures and functions for parsing and representing values from RESP as
//! message frame a network environment

use std::io::Cursor;

use bytes::{Buf, Bytes};
use thiserror::Error;

/// Error from parsing a frame
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    /// There's not enough bytes to form a frame
    #[error("Incomplete frame")]
    Incomplete,

    /// Unexpected bytes encountered during parse
    #[error("Invalid frame encoding")]
    BadEncoding,

    /// Could not read bytes as integer
    #[error("Could not parse bytes as an integer (got {0})")]
    NotInteger(String),

    /// Could not read bytes as utf8 string
    #[error("Could not parse bytes as an UTF-8 string - {0}")]
    NotUtf8(#[from] std::string::FromUtf8Error),
}

/// A frame in [Redis Serialization Protocol (RESP)].
///
/// This is the smallest data unit that is accepted by the client and ther server when
/// they communicate over the network.
///
/// [Redis Serialization Protocol (RESP)]: https://redis.io/topics/protocol
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// An UTF-8 string that does not contain carriage-return nor line-feed used for sending
    /// general information.
    SimpleString(String),
    /// An UTF-8 string that does not contain carriage-return nor line-feed used for sending errors
    /// that occured.
    Error(String),
    /// A signed 64-bit number.
    Integer(i64),
    /// A bytes sequence.
    BulkString(Bytes),
    /// A sequence of frames.
    Array(Vec<Frame>),
    /// Nothingness
    Null,
}

impl Frame {
    /// Try to read data of a frame from the given reader.
    ///
    /// Returns the frame if it can be parsed from the reader, otherwise, returns an error.
    /// The error variant [`FrameError::Incomplete`] indicates that the reader does not have
    /// enough data for the frame; caller should retry later after receiving this error.
    ///
    /// [`FrameError::Incomplete`]: crate::resp::frame::Error::Incomplete
    pub fn parse(reader: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        match get_byte(reader)? {
            b'+' => {
                let l = get_line(reader)?;
                let s = String::from_utf8(l.to_vec())?;
                Ok(Frame::SimpleString(s))
            }
            b'-' => {
                let l = get_line(reader)?;
                let e = String::from_utf8(l.to_vec())?;
                Ok(Frame::Error(e))
            }
            b':' => {
                let x = get_integer(reader)?;
                Ok(Frame::Integer(x))
            }
            b'$' => {
                if peek_byte(reader)? == b'-' {
                    // If there's a "-1", it's a null frame.
                    // Otherwise, a negative length is invalid
                    let l = get_line(reader)?;
                    if l != b"-1" {
                        return Err(Error::BadEncoding);
                    }
                    return Ok(Frame::Null);
                }
                // Parse the bulk string length and try convert it to u64
                let len = get_integer(reader)?;
                let len = len.try_into().map_err(|_| Error::BadEncoding)?;
                if (len + 2) > reader.remaining() {
                    // Missing \r\n
                    return Err(Error::Incomplete);
                }
                // Get the bulk string
                let b = reader.copy_to_bytes(len);
                skip(reader, 2)?; // skip \r\n
                Ok(Frame::BulkString(b))
            }
            b'*' => {
                // Parse the array length and try convert it to u64
                let len = get_integer(reader)?;
                let len = len.try_into().map_err(|_| Error::BadEncoding)?;
                // Recursively parse each element of the array
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(Frame::parse(reader)?);
                }
                Ok(Frame::Array(items))
            }
            _ => Err(Error::BadEncoding),
        }
    }

    /// Checks if a message frame can be parsed from the reader without memory allocations.
    pub fn check(buf: &mut Cursor<&[u8]>) -> Result<(), Error> {
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
                if peek_byte(buf)? == b'-' {
                    // Should be a null frame
                    skip(buf, 4)?; // skip '-1\r\n'
                } else {
                    let n = get_integer(buf)?;
                    let n: usize = n.try_into().map_err(|_| Error::BadEncoding)?;
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
            _ => return Err(Error::BadEncoding),
        }
        Ok(())
    }
}

/// Read until we encounter '\r' then skip 2 spaces for '\r\n'. Fails when there's no '\r' or
/// there's a '\n'
fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;
    for i in start..end {
        match buf.get_ref()[i] {
            b'\r' => {
                buf.advance(i - start + 2);
                return Ok(&buf.get_ref()[start..i]);
            }
            b'\n' => return Err(Error::BadEncoding),
            _ => {}
        }
    }
    Err(Error::Incomplete)
}

fn get_integer(buf: &mut Cursor<&[u8]>) -> Result<i64, Error> {
    let buf_init_pos = buf.position() as usize;
    // check of sign byte
    let is_positive = match peek_byte(buf)? {
        b'-' => {
            buf.advance(1);
            false
        }
        b'+' => {
            buf.advance(1);
            true
        }
        // we don't care if the first byte is a digit or not
        _ => true,
    };

    // i64 has at most 19 digits, so we parse the first 18 digits using unchecked arithmetic
    // and parse the last few digits using checked arithmetic
    let max_safe_digits = 18;
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;

    let mut idx = start;
    let mut num: i64 = 0;

    // using if clause improves performance over multiplying with the sign value
    let num = if is_positive {
        // parse unchecked
        while idx != end && idx != max_safe_digits {
            match ascii_to_i64(buf.get_ref()[idx]) {
                Some(n) => num = num * 10 + n,
                None => break,
            }
            idx += 1;
        }
        // parse checked
        let mut num = Some(num);
        while idx != end {
            match ascii_to_i64(buf.get_ref()[idx]) {
                Some(n) => {
                    num = num
                        .and_then(|num| num.checked_mul(10))
                        .and_then(|num| num.checked_add(n))
                }
                None => break,
            }
            idx += 1;
        }
        num
    } else {
        // parse unchecked
        while idx != end && idx != max_safe_digits {
            match ascii_to_i64(buf.get_ref()[idx]) {
                Some(n) => num = num * 10 - n,
                None => break,
            }
            idx += 1;
        }
        // parse checked
        let mut num = Some(num);
        while idx != end {
            match ascii_to_i64(buf.get_ref()[idx]) {
                Some(n) => {
                    num = num
                        .and_then(|num| num.checked_mul(10))
                        .and_then(|num| num.checked_sub(n))
                }
                None => break,
            }
            idx += 1;
        }
        num
    };

    if idx == end {
        return Err(Error::Incomplete);
    }
    if idx == start || buf.get_ref()[idx] != b'\r' {
        // fails when there's no digit or when the integer is not ended with "\r\n"
        return Err(Error::NotInteger(
            String::from_utf8_lossy(&buf.get_ref()[idx..=end]).to_string(),
        ));
    }
    buf.advance(idx - start + 2); // skip "\r\n"
    num.ok_or_else(|| {
        Error::NotInteger(String::from_utf8_lossy(&buf.get_ref()[buf_init_pos..idx]).to_string())
    })
}

fn get_byte(buf: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !buf.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(buf.get_u8())
}

fn peek_byte(src: &Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

fn ascii_to_i64(b: u8) -> Option<i64> {
    if !(48..=57).contains(&b) {
        return None;
    }
    Some(b as i64 - 48)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn parse_simple_string_ok() {
        assert_frame(b"+OK\r\n", Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_simple_string_incomplete() {
        assert_frame_error(b"+", Error::Incomplete);
        assert_frame_error(b"+O", Error::Incomplete);
        assert_frame_error(b"+OK", Error::Incomplete);
        assert_frame_error(b"+OK\r", Error::Incomplete);
    }

    #[test]
    fn parse_simple_string_ignoring_carriage_return() {
        // extraneous '\r' will not affect parsing of single frame
        assert_frame(b"+OK\r\r\n", Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_simple_string_fails_because_line_feed() {
        assert_frame_error(b"+OK\n\r\n", Error::BadEncoding);
    }

    #[test]
    fn parse_error_ok() {
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
    fn parse_error_incomplete() {
        assert_frame_error(b"-E", Error::Incomplete);
        assert_frame_error(b"-ER", Error::Incomplete);
        assert_frame_error(b"-ER\r", Error::Incomplete);
    }

    #[test]
    fn parse_error_ignoring_carriage_return() {
        // extraneous '\r' will not affect parsing of single frame
        assert_frame(b"-Error test\r\r\n", Frame::Error("Error test".to_string()));
    }

    #[test]
    fn parse_error_fails_because_line_feed() {
        assert_frame_error(b"-Error test\n\r\n", Error::BadEncoding);
    }

    #[test]
    fn parse_integer_ok() {
        assert_frame(b":1000\r\n", Frame::Integer(1000));
        assert_frame(b":-100\r\n", Frame::Integer(-100));
    }

    #[test]
    fn parse_integer_incomplete() {
        assert_frame_error(b":", Error::Incomplete);
        assert_frame_error(b":1", Error::Incomplete);
        assert_frame_error(b":11", Error::Incomplete);
        assert_frame_error(b":11\r", Error::Incomplete);
    }

    #[test]
    fn parse_integer_fails_when_line_there_is_no_digit() {
        assert_frame_error(b":\r\n", Error::NotInteger("\r\n".into()));
    }

    #[test]
    fn parse_integer_fails_when_there_is_non_digit() {
        assert_frame_error(b":nan\r\n", Error::NotInteger("nan\r\n".into()));
    }

    #[test]
    fn parse_integer_fails_when_value_underflow() {
        assert_frame_error(
            b":-9223372036854775809\r\n",
            Error::NotInteger("-9223372036854775809".into()),
        );
    }

    #[test]
    fn parse_integer_fails_when_value_overflow() {
        assert_frame_error(
            b":9223372036854775808\r\n",
            Error::NotInteger("9223372036854775808".into()),
        );
    }

    #[test]
    fn parse_bulk_string_ok() {
        assert_frame(b"$5\r\nhello\r\n", Frame::BulkString("hello".into()));
        assert_frame(b"$0\r\n\r\n", Frame::BulkString(Bytes::new()));

        // parse bulk strings based on length prefix ignoring any sequence of "\r\n"
        assert_frame(
            b"$11\r\nhello\rworld\r\n",
            Frame::BulkString("hello\rworld".into()),
        );
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
    fn parse_bulk_string_incomplete() {
        assert_frame_error(b"$", Error::Incomplete);
        assert_frame_error(b"$2", Error::Incomplete);
        assert_frame_error(b"$2\r", Error::Incomplete);
        assert_frame_error(b"$2\r\n", Error::Incomplete);
        assert_frame_error(b"$2\r\no", Error::Incomplete);
        assert_frame_error(b"$2\r\nok", Error::Incomplete);
        assert_frame_error(b"$2\r\nok\r", Error::Incomplete);
    }

    #[test]
    fn parse_bulk_string_fails_with_invalid_length_prefix() {
        assert_frame_error(b"$-2\r\n", Error::BadEncoding);
    }

    #[test]
    fn parse_array_ok() {
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
    fn parse_array_incomplete() {
        assert_frame_error(b"*", Error::Incomplete);
        assert_frame_error(b"*1", Error::Incomplete);
        assert_frame_error(b"*1\r", Error::Incomplete);
        assert_frame_error(b"*1\r\n", Error::Incomplete);
        assert_frame_error(b"*1\r\n:", Error::Incomplete);
        assert_frame_error(b"*1\r\n:1", Error::Incomplete);
        assert_frame_error(b"*1\r\n:1\r", Error::Incomplete);
    }

    #[test]
    fn parse_array_length_fails_with_invalid_length_prefix() {
        assert_frame_error(b"*-2\r\n", Error::BadEncoding);
    }

    #[test]
    fn parse_null_ok() {
        assert_frame(b"$-1\r\n", Frame::Null);
    }

    #[test]
    fn parse_null_incomplete() {
        assert_frame_error(b"$-", Error::Incomplete);
        assert_frame_error(b"$-1", Error::Incomplete);
        assert_frame_error(b"$-1\r", Error::Incomplete);
    }

    #[test]
    fn parse_null_fails_when_use_array_variant() {
        assert_frame_error(b"*-1\r\n", Error::BadEncoding);
    }

    fn assert_frame(input_data: &[u8], expected_frame: Frame) {
        let frame = parse_frame(input_data).unwrap();
        assert_eq!(frame, expected_frame);
    }

    fn assert_frame_error(input_data: &[u8], expected_err: Error) {
        let err = parse_frame(input_data).unwrap_err();
        assert_eq!(expected_err, err)
    }

    fn parse_frame(input_data: &[u8]) -> Result<Frame, Error> {
        let mut buf = Cursor::new(input_data);

        Frame::check(&mut buf)?;
        buf.set_position(0);

        Frame::parse(&mut buf)
    }
}
