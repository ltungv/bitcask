use std::{
    fs,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

/// A wrapper a round `BufReader` that keeps track of the last read position.
#[derive(Debug)]
pub struct BufReaderWithPos<R>
where
    R: Read,
{
    pos: u64,
    reader: BufReader<R>,
}

impl<R> BufReaderWithPos<R>
where
    R: Read + Seek,
{
    /// Create a new buffered reader.
    pub fn new(mut r: R) -> io::Result<Self> {
        let pos = r.seek(SeekFrom::Current(0))?;
        let reader = BufReader::new(r);
        Ok(Self { pos, reader })
    }

    /// Return the last read position.
    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<R> Read for BufReaderWithPos<R>
where
    R: Read,
{
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.reader.read(b).map(|bytes_read| {
            self.pos += bytes_read as u64;
            bytes_read
        })
    }
}
impl<R> Seek for BufReaderWithPos<R>
where
    R: Read + Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos).map(|posn| {
            self.pos = posn;
            posn
        })
    }
}

/// A wrapper a round `BufWriter` that keeps track of the last written position.
#[derive(Debug)]
pub struct BufWriterWithPos<W>
where
    W: Write,
{
    pos: u64,
    writer: BufWriter<W>,
}

impl<W> BufWriterWithPos<W>
where
    W: Write + Seek,
{
    /// Create a new buffered writer.
    pub fn new(mut w: W) -> io::Result<Self> {
        let pos = w.seek(SeekFrom::End(0))?;
        let writer = BufWriter::new(w);
        Ok(Self { pos, writer })
    }

    /// Return the last written postion.
    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl<W> Write for BufWriterWithPos<W>
where
    W: Write,
{
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.writer.write(b).map(|bytes_written| {
            self.pos += bytes_written as u64;
            bytes_written
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W> Seek for BufWriterWithPos<W>
where
    W: Write + Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.writer.seek(pos).map(|posn| {
            self.pos = posn;
            posn
        })
    }
}

impl BufWriterWithPos<fs::File> {
    pub fn sync_all(&mut self) -> io::Result<()> {
        self.writer.get_ref().sync_all()
    }
}
