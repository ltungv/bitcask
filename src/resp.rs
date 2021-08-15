use bytes::{Buf, Bytes};

const MAX_BULK_STRING_LENGTH: usize = 512 * (1 << 20); // 512MB

/// Data types as specified in [Redis Protocol (RESP)]
///
/// [Redis Protocol (RESP)]: https://redis.io/topics/protocol
#[derive(Debug, PartialEq, Clone)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<Frame>),
    Null,
}

pub fn parse<R: Buf>(reader: &mut R) -> Result<Frame, FrameError> {
    let frame = match get_u8(reader)? {
        b'+' => parse_simple_string(reader)?,
        b'-' => parse_error(reader)?,
        b':' => parse_integer(reader)?,
        b'$' => parse_bulk_string(reader)?,
        b'*' => parse_array(reader)?,
        b => return Err(FrameError::InvalidByte(b)),
    };
    Ok(frame)
}

fn parse_simple_string<R: Buf>(reader: &mut R) -> Result<Frame, FrameError> {
    let ss = get_line(reader)?;
    Ok(Frame::SimpleString(ss))
}

fn parse_error<R: Buf>(reader: &mut R) -> Result<Frame, FrameError> {
    let err = get_line(reader)?;
    Ok(Frame::Error(err))
}

fn parse_integer<R: Buf>(reader: &mut R) -> Result<Frame, FrameError> {
    let int_buf = get_line(reader)?;
    let int_val = int_buf.parse::<i64>()?;
    Ok(Frame::Integer(int_val))
}

fn parse_bulk_string<R: Buf>(mut reader: R) -> Result<Frame, FrameError> {
    let bulk_len_buf = get_line(&mut reader)?;
    let bulk_len = bulk_len_buf.parse::<i64>()?;

    if bulk_len == -1 {
        return Ok(Frame::Null);
    }
    if bulk_len < 0 {
        return Err(FrameError::InvalidFormat);
    }

    let mut bulk_content = Vec::with_capacity(reader.remaining());
    let mut bytes_remain = bulk_len;
    while bytes_remain > 0 {
        bulk_content.push(get_u8(&mut reader)?);
        bytes_remain -= 1;
    }

    let cr = get_u8(&mut reader)?;
    if cr != b'\r' {
        return Err(FrameError::InvalidByte(cr));
    }
    let lf = get_u8(&mut reader)?;
    if lf != b'\n' {
        return Err(FrameError::InvalidByte(lf));
    }

    Ok(Frame::BulkString(bulk_content.into()))
}

fn parse_array<R: Buf>(reader: &mut R) -> Result<Frame, FrameError> {
    let array_len_buf = get_line(reader)?;
    let array_len = array_len_buf.parse::<i64>()?;

    if array_len == -1 {
        return Ok(Frame::Null);
    }
    if array_len < 0 {
        return Err(FrameError::InvalidFormat);
    }

    let mut items = Vec::new();
    let mut items_remain = array_len;
    while items_remain > 0 {
        items.push(parse(reader)?);
        items_remain -= 1;
    }
    Ok(Frame::Array(items))
}

fn get_line<R: Buf>(reader: &mut R) -> Result<String, FrameError> {
    let remaining_bytes = reader.remaining();
    let mut line_buf = Vec::with_capacity(remaining_bytes);
    loop {
        match get_u8(reader)? {
            b'\r' => {
                if let Some(b'\n') = peek(reader) {
                    reader.advance(1);
                    let ss = String::from_utf8(line_buf)?;
                    return Ok(ss);
                }
                return Err(FrameError::InvalidByte(b'\r'));
            }
            b'\n' => {
                return Err(FrameError::InvalidByte(b'\n'));
            }
            b => line_buf.push(b),
        }
    }
}

fn get_u8<R: Buf>(reader: &mut R) -> Result<u8, FrameError> {
    if !reader.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    Ok(reader.get_u8())
}

fn peek<R: Buf>(reader: &R) -> Option<u8> {
    if !reader.has_remaining() {
        return None;
    }
    Some(reader.chunk()[0])
}

#[derive(PartialEq)]
pub enum FrameError {
    Incomplete,
    InvalidByte(u8),
    InvalidFormat,

    FromUtf8Error(std::string::FromUtf8Error),
    ParseIntError(std::num::ParseIntError),
}

impl std::error::Error for FrameError {}

impl std::fmt::Debug for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            FrameError::Incomplete => write!(f, "Incomplete frame"),
            FrameError::InvalidByte(b) => write!(f, "Invalid byte {:x}", b),
            FrameError::InvalidFormat => write!(f, "Invalid frame format"),
            FrameError::FromUtf8Error(e) => write!(f, "{}", e),
            FrameError::ParseIntError(e) => write!(f, "{}", e),
        }
    }
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            FrameError::Incomplete => write!(f, "Incomplete frame"),
            FrameError::InvalidByte(b) => write!(f, "Invalid byte {:x}", b),
            FrameError::InvalidFormat => write!(f, "Invalid frame format"),
            FrameError::FromUtf8Error(e) => write!(f, "{}", e),
            FrameError::ParseIntError(e) => write!(f, "{}", e),
        }
    }
}

impl From<std::string::FromUtf8Error> for FrameError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        FrameError::FromUtf8Error(err)
    }
}

impl From<std::num::ParseIntError> for FrameError {
    fn from(err: std::num::ParseIntError) -> Self {
        FrameError::ParseIntError(err)
    }
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
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn parse_simple_string_with_cr() {
        init();
        let mut buf = Cursor::new(b"+OK\r\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert_eq!(parse_err, FrameError::InvalidByte(b'\r'));
    }

    #[test]
    fn parse_simple_string_with_lf() {
        init();
        let mut buf = Cursor::new(b"+OK\n\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert_eq!(parse_err, FrameError::InvalidByte(b'\n'));
    }

    #[test]
    fn parse_error_valid() {
        init();

        // Some examples from Redis

        let mut buf = Cursor::new(b"-Error test\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Error("Error test".to_string()));

        let mut buf = Cursor::new(b"-ERR unknown command 'foobar'\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Error("ERR unknown command 'foobar'".to_string())
        );

        let mut buf =
            Cursor::new("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        let frame = parse(&mut buf).unwrap();
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
        let parse_err = parse(&mut buf).unwrap_err();
        assert_eq!(parse_err, FrameError::InvalidByte(b'\r'));
    }

    #[test]
    fn parse_error_with_lf() {
        init();
        let mut buf = Cursor::new(b"-Error test\n\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert_eq!(parse_err, FrameError::InvalidByte(b'\n'));
    }

    #[test]
    fn parse_integer_valid() {
        init();
        let mut buf = Cursor::new(b":1000\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Integer(1000));
    }

    #[test]
    fn parse_integer_empty() {
        init();
        let mut buf = Cursor::new(b":\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, FrameError::ParseIntError(_)));
    }

    #[test]
    fn parse_integer_nan() {
        init();
        let mut buf = Cursor::new(b":nan\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, FrameError::ParseIntError(_)));
    }

    #[test]
    fn parse_integer_underflow() {
        init();
        let mut buf = Cursor::new(b":-9223372036854775809\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, FrameError::ParseIntError(_)));
    }

    #[test]
    fn parse_integer_overflow() {
        init();
        let mut buf = Cursor::new(b":9223372036854775808\r\n");
        let parse_err = parse(&mut buf).unwrap_err();
        assert!(matches!(parse_err, FrameError::ParseIntError(_)));
    }

    #[test]
    fn parse_bulk_string_valid_simple() {
        init();
        let mut buf = Cursor::new(b"$5\r\nhello\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello".into()));
    }

    #[test]
    fn parse_bulk_string_valid_empty() {
        init();
        let mut buf = Cursor::new(b"$0\r\n\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn parse_bulk_string_valid_null() {
        init();
        let mut buf = Cursor::new(b"$-1\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn parse_bulk_string_valid_contains_line_feed() {
        init();
        let mut buf = Cursor::new(b"$11\r\nhello\nworld\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello\nworld".into()));
    }

    #[test]
    fn parse_bulk_string_valid_contains_crlf() {
        init();
        let mut buf = Cursor::new(b"$12\r\nhello\r\nworld\r\n");

        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::BulkString("hello\r\nworld".into()));
    }

    #[test]
    fn parse_bulk_string_length_invalid() {
        init();
        let mut buf = Cursor::new(b"$-2\r\n");
        let err = parse(&mut buf).unwrap_err();
        assert_eq!(err, FrameError::InvalidFormat);
    }

    #[test]
    fn parse_bulk_string_length_prefix_larger_than_content() {
        init();
        let mut buf = Cursor::new(b"$24\r\nhello\r\nworld\r\n");
        let err = parse(&mut buf).unwrap_err();
        assert_eq!(err, FrameError::Incomplete);
    }

    #[test]
    fn parse_bulk_string_length_prefix_smaller_than_content() {
        init();
        let mut buf = Cursor::new(b"$6\r\nhello\r\nworld\r\n");

        let err = parse(&mut buf).unwrap_err();
        assert_eq!(err, FrameError::InvalidByte(b'\n'));
    }

    #[test]
    fn parse_array_valid_multi_cases() {
        let mut buf = Cursor::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString("foo".into()),
                Frame::BulkString("bar".into())
            ])
        );

        let mut buf = Cursor::new("*3\r\n:1\r\n:2\r\n:3\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3)
            ])
        );

        let mut buf = Cursor::new("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n");
        let frame = parse(&mut buf).unwrap();
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
        let frame = parse(&mut buf).unwrap();
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
        let frame = parse(&mut buf).unwrap();
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

        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Array(vec![]));
    }

    #[test]
    fn parse_array_valid_null() {
        init();
        let mut buf = Cursor::new(b"*-1\r\n");
        let frame = parse(&mut buf).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn parse_array_length_invalid_format() {
        init();
        let mut buf = Cursor::new(b"*-2\r\n");
        let err = parse(&mut buf).unwrap_err();
        assert_eq!(err, FrameError::InvalidFormat);
    }
}
