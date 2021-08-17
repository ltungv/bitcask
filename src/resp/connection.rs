use bytes::BytesMut;
use tokio::{io::BufWriter, net::TcpStream};

use super::{Error, Frame};

#[derive(Debug)]
pub struct Connection {
    ostream: BufWriter<TcpStream>,
    buffer: BytesMut, // buffered reader
}

impl Connection {
    pub fn new(tcp: TcpStream) -> Self {
        Self {
            ostream: BufWriter::new(tcp),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    pub fn read_frame(&mut self) -> Result<Frame, Error> {
        todo!()
    }

    pub fn write_frame(&mut self) -> Result<(), Error> {
        todo!()
    }
}
