use std::io::{self, Cursor, Write};

use bytes::{Buf, BytesMut};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use super::frame::{Frame, FrameError};

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Frame error - {0}")]
    Frame(#[from] FrameError),

    #[error("I/O error - {0}")]
    Io(#[from] io::Error),
}

/// Sends and receives [`Frame`] values from the remote peer.
///
/// [`Frame`]: crate::resp::Frame;
pub struct Connection<S = TcpStream> {
    // wraps a stream inside a BufWriter to reduce the number of write syscalls
    stream: BufWriter<S>,
    // buffered data from read operation
    buffer: BytesMut,
}

impl<S> Connection<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    /// Creates a new connection over the given readable and writable stream, then
    /// initializes the inner read/write buffers
    pub fn new(stream: S) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    /// Reads the available frame from the underlying buffer
    ///
    /// Returns the received frame if succeeded. When the underlying stream is
    /// closed and there's no data left to be read, returns `None`. Otherwise,
    /// an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    // Peer closed when all data is parsed
                    return Ok(None);
                } else {
                    // The peer closed the socket while sending a frame.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "connection reset by peer",
                    )
                    .into());
                }
            }
        }
    }

    /// Write a frame to the underlying stream
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        if let Frame::Array(items) = frame {
            self.write_array(items).await?;
        } else {
            self.write_single_value(frame).await?;
        }

        self.stream.flush().await?;
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buf) {
            Ok(()) => {
                // Get the byte length of the frame
                let len = buf.position() as usize;

                // Parse the frame
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;

                // Discard the frame from the buffer
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // Not enough data has been buffered
            Err(FrameError::Incomplete) => Ok(None),
            // An error was encountered
            Err(e) => Err(e.into()),
        }
    }

    async fn write_array(&mut self, items: &[Frame]) -> Result<(), ConnectionError> {
        // frame init
        self.stream.write_u8(b'*').await?;

        // send frame content
        let len = items.len();
        self.write_decimal(len as i64).await?;
        self.stream.write_all(b"\r\n").await?;

        for item in items {
            self.write_single_value(item).await?;
        }
        Ok(())
    }

    async fn write_single_value(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        match frame {
            Frame::SimpleString(s) => {
                // frame init
                self.stream.write_u8(b'+').await?;

                // send string content
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(e) => {
                // frame init
                self.stream.write_u8(b'-').await?;

                // send error content
                self.stream.write_all(e.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(i) => {
                // frame init
                self.stream.write_u8(b':').await?;

                // send integer as digits
                self.write_decimal(*i).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::BulkString(bs) => {
                // frame init
                self.stream.write_u8(b'$').await?;

                // send bulk's length as digits
                let len = bs.len();
                self.write_decimal(len as i64).await?;
                self.stream.write_all(b"\r\n").await?;

                // send bulk's content
                self.stream.write_all(bs).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_) => unimplemented!(),
        }
        Ok(())
    }

    async fn write_decimal(&mut self, value: i64) -> Result<(), ConnectionError> {
        // i64 has about 20 digits
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        // write the integer as string
        write!(&mut buf, "{}", value)?;
        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::io::Cursor;

    #[tokio::test]
    async fn write_frame_check_sent_buffer() -> Result<(), Box<dyn std::error::Error>> {
        for test_case in get_test_cases() {
            assert_frame_write(test_case.0, test_case.1).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_frame_check_received_frame() -> Result<(), Box<dyn std::error::Error>> {
        for test_case in get_test_cases() {
            assert_frame_read(test_case.1, test_case.0).await?;
        }
        Ok(())
    }

    async fn assert_frame_write(
        frame: Frame,
        expected_buffer: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = Cursor::new(Vec::new());
        let mut conn = Connection::new(&mut stream);

        conn.write_frame(&frame).await?;
        assert_eq!(stream.get_ref(), expected_buffer);

        Ok(())
    }

    async fn assert_frame_read(
        buf: &[u8],
        expected_frame: Frame,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = Cursor::new(Vec::from(buf));
        let mut conn = Connection::new(&mut stream);

        let frame = conn.read_frame().await?;
        assert_eq!(frame, Some(expected_frame));

        Ok(())
    }

    fn get_test_cases() -> Vec<(Frame, &'static [u8])> {
        vec![
            // simple strings
            (Frame::SimpleString("OK".to_string()), b"+OK\r\n".as_slice()),
            // errors
            (
                Frame::Error("Error test".to_string()),
                b"-Error test\r\n".as_slice(),
            ),
            (
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    .as_slice(),
            ),
            (
                Frame::Error("ERR unknown command 'foobar'".to_string()),
                b"-ERR unknown command 'foobar'\r\n".as_slice(),
            ),
            // bulk strings
            (
                Frame::BulkString("hello".into()),
                b"$5\r\nhello\r\n".as_slice(),
            ),
            (Frame::BulkString(Bytes::new()), b"$0\r\n\r\n".as_slice()),
            (
                Frame::BulkString("hello\nworld".into()),
                b"$11\r\nhello\nworld\r\n.as_slice()",
            ),
            (
                Frame::BulkString("hello\r\nworld".into()),
                b"$12\r\nhello\r\nworld\r\n.as_slice()",
            ),
            // arrays
            (Frame::Array(vec![]), b"*0\r\n".as_slice()),
            (
                Frame::Array(vec![
                    Frame::BulkString("foo".into()),
                    Frame::BulkString("bar".into()),
                ]),
                b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_slice(),
            ),
            (
                Frame::Array(vec![
                    Frame::Integer(1),
                    Frame::Integer(2),
                    Frame::Integer(3),
                ]),
                b"*3\r\n:1\r\n:2\r\n:3\r\n".as_slice(),
            ),
            (
                Frame::Array(vec![
                    Frame::Integer(1),
                    Frame::Integer(2),
                    Frame::Integer(3),
                    Frame::Integer(4),
                    Frame::BulkString("foobar".into()),
                ]),
                b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n".as_slice(),
            ),
            (
                Frame::Array(vec![
                    Frame::BulkString("foo".into()),
                    Frame::Null,
                    Frame::BulkString("bar".into()),
                ]),
                b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n".as_slice(),
            ),
            // null
            // NOTE: We use the bulk string representation for null
            (Frame::Null, b"$-1\r\n".as_slice()),
        ]
    }
}
