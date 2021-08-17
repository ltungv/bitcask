use std::io::{Cursor, Write};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use super::{Error, Frame};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut, // buffered reader
}

impl Connection {
    pub fn new(tcp: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(tcp),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Error> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    // The peer closed the socket while sending a frame.
                    return Err(Error::ConnectionReset);
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        match frame {
            Frame::SimpleString(s) => {
                // frame init
                self.stream.write_u8(b'+').await?;
                // send frame content
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(e) => {
                // frame init
                self.stream.write_u8(b'-').await?;
                // send frame content
                self.stream.write_all(e.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(i) => {
                // frame init
                self.stream.write_u8(b':').await?;

                // send frame content
                self.write_decimal(*i).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::BulkString(bs) => {
                // frame init
                self.stream.write_u8(b'$').await?;
                // send frame content
                let len = bs.len();
                self.write_decimal(len as i64).await?;
                self.stream.write_all(bs).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!(),
        }

        self.stream.flush().await?;
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, Error> {
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
            Err(Incomplete) => Ok(None),
            // An error was encountered
            Err(e) => Err(e),
        }
    }

    async fn write_decimal(&mut self, value: i64) -> Result<(), Error> {
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
