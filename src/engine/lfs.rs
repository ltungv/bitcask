//! Implementation of a `KeyValueStore` that uses log-structured file system to manage data and
//! persist data to disk with high write throughput.

use super::KeyValueStore;
use crate::error::{Error, ErrorKind};
use bytes::Bytes;
use crossbeam::{queue::ArrayQueue, utils::Backoff};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

/// Merge log files when then number of unused bytes across all files exceeds this limit.
const GARBAGE_THRESHOLD: u64 = 4 * 1024 * 1024; // 4MB

/// A simple string key-value store that persists data to on-disk log files and keeps indices
/// to the locations of the persisted data in the log files using a hash table for fast queries.
///
/// # Design notes
///
/// We use 2 separated objects of types  `WriteContext` and `ReadContext` so that read
/// operations can occur concurrently without having to use a lock. We ensure that:
/// + The index remains consistent while data is being persisted to on-disk logs.
/// + Reads from index and from disk can happen on multiple threads at once.
/// + Reads can happen in parallel with writes, which means that `r_contexts` always see a
/// consistent state.
/// + On-disk data is periodically compacted while maintaining the invariants for reads.
///
/// We maintain a queue of `ReadContext` in `r_contexts` so we don't create multiple copies of
/// `ReadContext` when there are multiple concurrent accesses. Because the number of concurrent
/// reads are limited by the machine hardware, we can be sure that there are at most `N` objects
/// of type `ReadContext` at once. Thus we can create `N` objects of type `ReadContext` upon
/// initialization and cycle through them when required. When a read occurs, a `ReadContext` is
/// taken from the queue, then it is put back into the queue once the read finishes.
///
/// # References
///
/// + [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
/// + [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
#[derive(Debug)]
pub struct LogStructuredHashTable {
    /// The write-context is in-charge of making modifications to the state of the key-value store.
    w_context: Arc<Mutex<WriteContext>>,

    /// The read-context is in-charge of providing access to the state of the key-value store.
    r_contexts: Arc<ArrayQueue<ReadContext>>,
}

impl LogStructuredHashTable {
    /// Open the persisted key-value store at the given path and return it to the caller.
    pub fn open<P>(path: P, concurrent_reads: usize) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let prev_gens = previous_gens(&path)?;
        let gen = prev_gens.last().map(|&e| e + 1).unwrap_or_default();

        let mut garbage = 0;
        let mut index = DashMap::new();

        // go through all log files, rebuild the index, and keep the reader to each log for later access
        for prev_gen in prev_gens {
            let mut reader = open_log(&path, prev_gen)?;
            garbage += build_index(&mut reader, &mut index, prev_gen)?;
        }
        // create a new log file for this instance and take a write handle and a read handle
        let (writer, _) = create_log(&path, gen)?;

        let context = Arc::new(Context {
            path: path.as_ref().to_path_buf(),
            merge_gen: AtomicU64::new(0),
            index,
        });

        let r_contexts = Arc::new(ArrayQueue::new(concurrent_reads));
        for _ in 0..concurrent_reads {
            r_contexts
                .push(ReadContext {
                    context: Arc::clone(&context),
                    readers: RefCell::new(BTreeMap::new()),
                })
                .expect("unreachable error");
        }

        let w_context = Arc::new(Mutex::new(WriteContext {
            context: Arc::clone(&context),
            readers: RefCell::new(BTreeMap::new()),
            writer,
            gen,
            garbage,
        }));

        Ok(Self {
            w_context,
            r_contexts,
        })
    }
}

impl KeyValueStore for LogStructuredHashTable {
    fn set(&self, key: String, val: Bytes) -> Result<(), Error> {
        self.w_context.lock().unwrap().set(key, val)
    }

    fn get(&self, key: &str) -> Result<Bytes, Error> {
        let backoff = Backoff::new();
        loop {
            if let Some(r_context) = self.r_contexts.pop() {
                // Make a query with the key and return the context to the queue after we finish so
                // other threads can make progress
                let result = r_context.get(key);
                self.r_contexts.push(r_context).expect("unreachable error");
                break result;
            }
            // Spin until we have access to a read context
            backoff.spin();
        }
    }

    fn del(&self, key: &str) -> Result<(), Error> {
        self.w_context.lock().unwrap().del(key)
    }
}

impl Clone for LogStructuredHashTable {
    fn clone(&self) -> Self {
        Self {
            w_context: Arc::clone(&self.w_context),
            r_contexts: Arc::clone(&self.r_contexts),
        }
    }
}

/// The context of the key-value store, i.e., shared state required by different components of
/// the system.
#[derive(Debug)]
struct Context {
    /// The path of the directory where log files are stored.
    path: PathBuf,

    /// The generation number of the last merged log file, i.e., the oldest generation number
    /// where data is not stale. We use this number to determine which readers to be discared.
    merge_gen: AtomicU64,

    /// The mapping from key strings to positions of data in the log files.
    index: DashMap<String, LogIndex>,
}

/// The database write context that updates on-disk log files and maintains a consistent index to
/// the locations of log entries on those files.
#[derive(Debug)]
struct WriteContext {
    /// The state that is shared by both `WriteContext` and `ReadContext`.
    context: Arc<Context>,

    /// The mapping from generation numbers to the log file readers used when performing merges.
    readers: RefCell<BTreeMap<u64, BufSeekReader<File>>>,

    /// The buffered writer the is used to persist on-disk data.
    writer: BufSeekWriter<File>,

    /// The generation number of the log file that is currently being written to.
    gen: u64,

    /// The amount of unused bytes across all log files accumulated when the value of a key is
    /// changed or when the key is deleted.
    garbage: u64,
}

impl WriteContext {
    /// Set the value of a key, overwriting any existing value at that key.
    ///
    /// # Error
    ///
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn set(&mut self, key: String, val: Bytes) -> Result<(), Error> {
        // Write the entry to disk
        let (pos, len) = self.write(LogEntry::Set(key.clone(), val))?;
        let log_index = LogIndex {
            gen: self.gen,
            pos,
            len,
        };
        // Accumulated unused bytes count when we overwrite the previous value of the given key
        if let Some(prev_index) = self.context.index.insert(key, log_index) {
            self.gc(prev_index.len)?;
        };
        Ok(())
    }

    /// Delete a key and its value, if it exists.
    ///
    /// # Error
    ///
    /// If the deleted key does not exist, returns an error of kind `ErrorKind::KeyNotFound`.
    /// Errors from I/O operations and serializations/deserializations will be propagated.
    fn del(&mut self, key: &str) -> Result<(), Error> {
        match self.context.index.remove(key) {
            Some((_, prev_index)) => {
                // Write a tombstone value
                self.write(LogEntry::Rm(key.to_string()))?;
                // Accumulated unused bytes count when we delete the given key
                self.gc(prev_index.len)?;
                Ok(())
            }
            None => Err(Error::from(ErrorKind::KeyNotFound)),
        }
    }

    fn write(&mut self, entry: LogEntry) -> Result<(u64, u64), Error> {
        let pos = self.writer.pos;
        bincode::serialize_into(&mut self.writer, &entry)
            .map_err(|e| Error::new(ErrorKind::SerializationFailed, e))?;
        self.writer.flush()?;
        let len = self.writer.pos - pos;
        Ok((pos, len))
    }

    fn gc(&mut self, sz: u64) -> Result<(), Error> {
        self.garbage += sz;
        if self.garbage > GARBAGE_THRESHOLD {
            self.merge()?;
        }
        Ok(())
    }

    fn merge(&mut self) -> Result<(), Error> {
        // Copy 2 new logs, one for merging and one for the new active log
        let merge_gen = self.gen + 1;
        let new_gen = self.gen + 2;
        let (mut merged_writer, merged_reader) = create_log(&self.context.path, merge_gen)?;
        let (writer, reader) = create_log(&self.context.path, new_gen)?;

        // Copy data to the merge log and update the index
        let mut readers = self.readers.borrow_mut();
        for mut log_index in self.context.index.iter_mut() {
            let reader = readers
                .entry(log_index.gen)
                .or_insert(open_log(&self.context.path, log_index.gen)?);

            reader.seek(SeekFrom::Start(log_index.pos))?;
            let mut entry_reader = reader.take(log_index.len);

            let merge_pos = merged_writer.pos;
            io::copy(&mut entry_reader, &mut merged_writer)?;

            *log_index = LogIndex {
                gen: merge_gen,
                pos: merge_pos,
                len: log_index.len,
            };
        }
        readers.insert(merge_gen, merged_reader);
        readers.insert(new_gen, reader);
        merged_writer.flush()?;

        // set merge generation, `ReadContext` in all threads will observe the new value and drop
        // its the file handle
        self.context.merge_gen.store(merge_gen, Ordering::SeqCst);

        // remove stale log files
        let prev_gens = previous_gens(&self.context.path)?;
        let stale_gens = prev_gens.iter().filter(|&&gen| gen < merge_gen);
        for gen in stale_gens {
            let log_path = self.context.path.join(format!("gen-{}.log", gen));
            fs::remove_file(log_path)?;
        }

        // update writer and log generation
        self.writer = writer;
        self.gen = new_gen;
        self.garbage = 0;
        Ok(())
    }
}

/// A database's reader that reads from on-disk files based on the current index
#[derive(Debug)]
struct ReadContext {
    context: Arc<Context>,
    readers: RefCell<BTreeMap<u64, BufSeekReader<File>>>,
}

impl ReadContext {
    fn get(&self, key: &str) -> Result<Bytes, Error> {
        match self.context.index.get(key) {
            None => Err(Error::from(ErrorKind::KeyNotFound)),
            Some(index) => {
                self.drop_stale_readers();
                Ok(self.get_by_index(index.value())?)
            }
        }
    }

    fn get_by_index(&self, index: &LogIndex) -> Result<Bytes, Error> {
        let log_entry = {
            let mut readers = self.readers.borrow_mut();
            let reader = readers
                .entry(index.gen)
                .or_insert(open_log(&self.context.path, index.gen)?);

            reader.seek(SeekFrom::Start(index.pos))?;
            bincode::deserialize_from(reader)
                .map_err(|e| Error::new(ErrorKind::DeserializationFailed, e))?
        };

        match log_entry {
            LogEntry::Set(_, value) => Ok(value),
            _ => Err(Error::new(
                ErrorKind::CorruptedLog,
                "Expecting a log entry for a set operation",
            )),
        }
    }

    fn drop_stale_readers(&self) {
        let merge_gen = self.context.merge_gen.load(Ordering::SeqCst);
        let mut readers = self.readers.borrow_mut();
        let gens: Vec<_> = readers
            .keys()
            .filter(|&g| *g < merge_gen)
            .cloned()
            .collect();
        gens.iter().for_each(|&gen| {
            readers.remove(&gen);
        });
    }
}

impl Clone for ReadContext {
    fn clone(&self) -> Self {
        // The `ReadContext` will be cloned and sent across threads. Each cloned `ReadContext`
        // will have unique file handles to the log files so that read can happen concurrently
        Self {
            context: Arc::clone(&self.context),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum LogEntry {
    Set(String, Bytes),
    Rm(String),
}

#[derive(Debug, Clone)]
struct LogIndex {
    gen: u64,
    pos: u64,
    len: u64,
}

fn build_index(
    reader: &mut BufSeekReader<File>,
    index_map: &mut DashMap<String, LogIndex>,
    gen: u64,
) -> Result<u64, Error> {
    reader.seek(SeekFrom::Start(0))?;
    let mut garbage = 0;
    loop {
        let pos = reader.pos;
        match bincode::deserialize_from(reader.by_ref()) {
            Ok(e) => match e {
                LogEntry::Set(key, _) => {
                    let len = reader.pos - pos;
                    let index = LogIndex { gen, pos, len };
                    if let Some(prev_index) = index_map.insert(key, index) {
                        garbage += prev_index.len;
                    };
                }
                LogEntry::Rm(key) => {
                    if let Some((_, prev_index)) = index_map.remove(&key) {
                        garbage += prev_index.len;
                    };
                }
            },
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                    // TODO: Note down why this is ok
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(Error::new(ErrorKind::DeserializationFailed, err)),
                },
                _ => return Err(Error::new(ErrorKind::DeserializationFailed, err)),
            },
        }
    }
    Ok(garbage)
}

fn open_log<P>(path: P, gen: u64) -> Result<BufSeekReader<File>, Error>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref().join(format!("gen-{}.log", gen));
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;
    let reader = BufSeekReader::new(readable_log)?;
    Ok(reader)
}

fn create_log<P>(path: P, gen: u64) -> Result<(BufSeekWriter<File>, BufSeekReader<File>), Error>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref().join(format!("gen-{}.log", gen));

    let writable_log = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&log_path)?;
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;

    let writer = BufSeekWriter::new(writable_log)?;
    let reader = BufSeekReader::new(readable_log)?;
    Ok((writer, reader))
}

fn previous_gens<P>(path: P) -> Result<Vec<u64>, Error>
where
    P: AsRef<Path>,
{
    let mut gens: Vec<u64> = fs::read_dir(&path)?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension() == Some("log".as_ref()))
        .filter_map(|p| {
            p.file_stem()
                .and_then(OsStr::to_str)
                .filter(|s| s.starts_with("gen-"))
                .map(|s| s.trim_start_matches("gen-"))
                .map(str::parse::<u64>)
        })
        .filter_map(std::result::Result::ok)
        .collect();
    gens.sort_unstable();
    Ok(gens)
}

#[derive(Debug)]
struct BufSeekWriter<W>
where
    W: Write,
{
    pos: u64,
    writer: BufWriter<W>,
}

impl<W> BufSeekWriter<W>
where
    W: Write,
{
    fn new(mut w: W) -> Result<Self, Error>
    where
        W: Write + Seek,
    {
        let pos = w.seek(SeekFrom::Current(0))?;
        let writer = BufWriter::new(w);
        Ok(Self { pos, writer })
    }
}

impl<W> Write for BufSeekWriter<W>
where
    W: Write,
{
    fn write(&mut self, b: &[u8]) -> std::result::Result<usize, io::Error> {
        self.writer.write(b).map(|bytes_written| {
            self.pos += bytes_written as u64;
            bytes_written
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
struct BufSeekReader<R>
where
    R: Read + Seek,
{
    pos: u64,
    reader: BufReader<R>,
}

impl<R> BufSeekReader<R>
where
    R: Read + Seek,
{
    fn new(mut r: R) -> Result<Self, Error> {
        let pos = r.seek(SeekFrom::Current(0))?;
        let reader = BufReader::new(r);
        Ok(Self { pos, reader })
    }
}

impl<R> Read for BufSeekReader<R>
where
    R: Read + Seek,
{
    fn read(&mut self, b: &mut [u8]) -> std::result::Result<usize, io::Error> {
        self.reader.read(b).map(|bytes_read| {
            self.pos += bytes_read as u64;
            bytes_read
        })
    }
}
impl<R> Seek for BufSeekReader<R>
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
