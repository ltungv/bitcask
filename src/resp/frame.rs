//! Data structures and functions for parsing and representing values from RESP as
//! message frame a network environment

use bytes::{Buf, Bytes};

use super::Error;

const MAX_BULK_STRING_LENGTH: i64 = 512 * (1 << 20); // 512MB

/// Data types as specified in [Redis Protocol (RESP)]
///
/// [Redis Protocol (RESP)]: https://redis.io/topics/protocol
#[derive(Debug, PartialEq, Clone)]
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
            _ => return Err(Error::InvalidFormat),
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
                    let data_len = n as usize + 2;
                    if data_len > reader.remaining() {
                        return Err(Error::Incomplete);
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
            _ => return Err(Error::InvalidFormat),
        }
        Ok(())
    }
}

fn parse_simple_string<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let line_length = get_line_length(reader)?;
    let simple_str = reader.copy_to_bytes(line_length - 2);
    reader.advance(2);

    let simple_str = String::from_utf8(simple_str.to_vec())?;
    Ok(Frame::SimpleString(simple_str))
}

fn parse_error<R: Buf>(reader: &mut R) -> Result<Frame, Error> {
    let line_length = get_line_length(reader)?;
    let error_str = reader.copy_to_bytes(line_length - 2);
    reader.advance(2);

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
        return Err(Error::InvalidFormat);
    }

    let bulk_len = bulk_len as usize;
    if (bulk_len + 2) > reader.remaining() {
        return Err(Error::Incomplete);
    }
    if reader.chunk()[bulk_len] != b'\r' || reader.chunk()[bulk_len + 1] != b'\n' {
        return Err(Error::InvalidFormat);
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
        return Err(Error::InvalidFormat);
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
    let reader_chunk = reader.chunk();
    let reader_bytes = reader.remaining();

    let mut is_negative = false;
    let mut int_value: Option<i64> = None;

    for i in 0..reader_bytes - 1 {
        match reader_chunk[i] {
            b'-' if i == 0 => is_negative = true,
            b if (b'0'..b'9').contains(&b) => {
                int_value = Some(
                    int_value
                        .unwrap_or(0)
                        .checked_mul(10)
                        .ok_or(Error::InvalidFormat)?
                        .checked_add((b - b'0') as i64)
                        .ok_or(Error::InvalidFormat)?,
                );
            }
            b'\r' => {
                if reader_chunk[i + 1] == b'\n' {
                    reader.advance(i + 2);
                    if let Some(int_value) = int_value {
                        if is_negative {
                            return Ok(int_value.checked_neg().ok_or(Error::InvalidFormat)?);
                        }
                        return Ok(int_value);
                    }
                }
                return Err(Error::InvalidFormat);
            }
            _ => return Err(Error::InvalidFormat),
        }
    }
    Err(Error::Incomplete)
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
                return Err(Error::InvalidFormat);
            }
            b'\n' => {
                return Err(Error::InvalidFormat);
            }
            _ => {}
        }
    }
    Err(Error::Incomplete)
}

fn get_byte<R: Buf>(reader: &mut R) -> Result<u8, Error> {
    if !reader.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(reader.get_u8())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Once;
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

    use super::*;

    fn init() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let env_filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace"));
            let fmt_layer = tracing_subscriber::fmt::Layer::new().with_writer(std::io::stdout);
            let subscriber = Registry::default().with(env_filter).with(fmt_layer);
            tracing::subscriber::set_global_default(subscriber)
                .expect("Unable to set a global subscriber");
        });
    }

    #[test]
    fn parse_simple_string_valid() {
        init();
        let mut buf = Cursor::new(b"+OK\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_simple_string_with_cr() {
        init();
        let mut buf = Cursor::new(b"+OK\r\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_simple_string_with_lf() {
        init();
        let mut buf = Cursor::new(b"+OK\n\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_error_valid() {
        init();

        // Some examples from Redis

        let mut buf = Cursor::new(b"-Error test\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Error("Error test".to_string()));

        let mut buf = Cursor::new(b"-ERR unknown command 'foobar'\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Error("ERR unknown command 'foobar'".to_string())
        );

        let mut buf =
            Cursor::new("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
            )
        );
    }

    #[test]
    fn parse_error_with_cr() {
        init();
        let mut buf = Cursor::new(b"-Error test\r\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_error_with_lf() {
        init();
        let mut buf = Cursor::new(b"-Error test\n\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_integer_valid() {
        init();
        let mut buf = Cursor::new(b":1000\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1000));
    }

    #[test]
    fn parse_integer_empty() {
        init();
        let mut buf = Cursor::new(b":\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_integer_nan() {
        init();
        let mut buf = Cursor::new(b":nan\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_integer_underflow() {
        init();
        let mut buf = Cursor::new(b":-9223372036854775809\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_integer_overflow() {
        init();
        let mut buf = Cursor::new(b":9223372036854775808\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_bulk_string_valid_simple() {
        init();
        let mut buf = Cursor::new(b"$5\r\nhello\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello".into()));
    }

    #[test]
    fn parse_bulk_string_valid_empty() {
        init();
        let mut buf = Cursor::new(b"$0\r\n\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn parse_bulk_string_valid_null() {
        init();
        let mut buf = Cursor::new(b"$-1\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn parse_bulk_string_valid_contains_line_feed() {
        init();
        let mut buf = Cursor::new(b"$11\r\nhello\nworld\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello\nworld".into()));
    }

    #[test]
    fn parse_bulk_string_valid_contains_crlf() {
        init();
        let mut buf = Cursor::new(b"$12\r\nhello\r\nworld\r\n");

        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello\r\nworld".into()));
    }

    #[test]
    fn parse_bulk_string_length_invalid() {
        init();
        let mut buf = Cursor::new(b"$-2\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let parse_err = Frame::parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_bulk_string_length_prefix_larger_than_content() {
        init();
        let mut buf = Cursor::new(b"$24\r\nhello\r\nworld\r\n");
        let parse_err = Frame::check(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::Incomplete));
    }

    #[test]
    fn parse_bulk_string_length_prefix_smaller_than_content() {
        init();
        let mut buf = Cursor::new(b"$6\r\nhello\r\nworld\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let parse_err = Frame::parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }

    #[test]
    fn parse_array_valid_multi_cases() {
        let mut buf = Cursor::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString("foo".into()),
                Frame::BulkString("bar".into())
            ])
        );

        let mut buf = Cursor::new("*3\r\n:1\r\n:2\r\n:3\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3)
            ])
        );

        let mut buf = Cursor::new("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
                Frame::Integer(4),
                Frame::BulkString("foobar".into())
            ])
        );

        let mut buf = Cursor::new("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::Integer(1),
                    Frame::Integer(2),
                    Frame::Integer(3),
                ]),
                Frame::Array(vec![
                    Frame::SimpleString("Foo".into()),
                    Frame::Error("Bar".into()),
                ])
            ])
        );

        let mut buf = Cursor::new("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString("foo".into()),
                Frame::Null,
                Frame::BulkString("bar".into())
            ]),
        );
    }

    #[test]
    fn parse_array_valid_empty() {
        init();
        let mut buf = Cursor::new(b"*0\r\n");

        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Array(vec![]));
    }

    #[test]
    fn parse_array_valid_null() {
        init();
        let mut buf = Cursor::new(b"*-1\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let frame = Frame::parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn parse_array_length_invalid_format() {
        init();
        let mut buf = Cursor::new(b"*-2\r\n");
        Frame::check(&mut buf).unwrap();
        buf.set_position(0);
        let parse_err = Frame::parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, Error::InvalidFormat));
    }
}
