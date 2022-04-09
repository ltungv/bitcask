use std::{ops::Range, path::Path, time};

use bytesize::ByteSize;

use super::Bitcask;

/// Configuration for a `Bitcask` instance. We try to mirror the configurations
/// available in [Configuring Bitcask].
///
/// [Configuring Bitcask]: https://docs.riak.com/riak/kv/latest/setup/planning/backend/bitcask/index.html#configuring-bitcask
#[derive(Debug, Clone)]
pub struct Config {
    pub(super) concurrency: usize,
    pub(super) max_file_size: ByteSize,
    pub(super) sync: SyncStrategy,
    pub(super) merge: MergeStrategy,
}

/// Control how data is synchronized to disk.
#[derive(Debug, Clone)]
pub enum SyncStrategy {
    /// Data is written to disk when the operating system flushes its buffers.
    None,
    /// Use the O_SYNC flags to force a synchronization after every write.
    OSync,
    /// Synchronize the the file system that the specified interval.
    Interval(time::Duration),
}

#[derive(Debug, Clone)]
pub(super) struct MergeStrategy {
    pub(super) enable: bool,
    pub(super) window: Range<u8>,
    pub(super) triggers: MergeTriggers,
    pub(super) thresholds: MergeThresholds,
    pub(super) check_inverval: time::Duration,
    pub(super) check_jitter: u8,
}

/// List of conditions that trigger the data files merging process
#[derive(Debug, Clone)]
pub(super) struct MergeTriggers {
    pub(super) fragmentation: u8,
    pub(super) dead_bytes: ByteSize,
}

/// pub(super) List of conditions that trigger the data files merging process
#[derive(Debug, Clone)]
pub(super) struct MergeThresholds {
    pub(super) fragmentation: u8,
    pub(super) dead_bytes: ByteSize,
    pub(super) small_file: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: num_cpus::get(),
            max_file_size: ByteSize::gib(2),
            sync: SyncStrategy::None,
            merge: MergeStrategy {
                enable: true,
                window: (0..24u8),
                check_inverval: time::Duration::from_secs(180),
                check_jitter: 30,
                triggers: MergeTriggers {
                    fragmentation: 60,
                    dead_bytes: ByteSize::mib(512),
                },
                thresholds: MergeThresholds {
                    fragmentation: 40,
                    dead_bytes: ByteSize::mib(128),
                    small_file: ByteSize::mib(10),
                },
            },
        }
    }
}

impl Config {
    /// Create a `Bitcask` instance at the given path with the available options.
    pub fn open<P>(self, path: P) -> Result<Bitcask, super::Error>
    where
        P: AsRef<Path>,
    {
        Bitcask::open(path, self)
    }

    /// Set the max number of concurrent readers. Default to the number of logical cores.
    pub fn concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the max file size. Default to `2GiBs`.
    pub fn max_file_size(&mut self, max_file_size: ByteSize) -> &mut Self {
        self.max_file_size = max_file_size;
        self
    }

    /// Set the synchronization strategy. Default to `SyncStrategy::None`.
    pub fn sync(&mut self, sync: SyncStrategy) -> &mut Self {
        self.sync = sync;
        self
    }

    /// Set whether data file will be merged. Default to `true`.
    pub fn merge(&mut self, enable: bool) -> &mut Self {
        self.merge.enable = enable;
        self
    }

    /// Set the merge policy to only merge during the given time window..
    pub fn merge_window(&mut self, window: Range<u8>) -> &mut Self {
        assert!((0..24).contains(&window.start));
        assert!((0..24).contains(&window.end));
        self.merge.window = window;
        self
    }

    /// Set the integer percentage of dead keys to total keys that will trigger a merge.
    /// Default to `60`.
    pub fn merge_trigger_fragmentation(&mut self, fragmentation: u8) -> &mut Self {
        self.merge.triggers.fragmentation = fragmentation;
        self
    }

    /// Set the minimum amount of bytes occupied by dead keys that will trigger a merge.
    /// Default to `60`.
    pub fn merge_trigger_dead_bytes(&mut self, dead_bytes: ByteSize) -> &mut Self {
        self.merge.triggers.dead_bytes = dead_bytes;
        self
    }

    /// Set the integer percentage of dead keys to total keys that will cause a data file to be
    /// included during a merge. Default to `40`.
    pub fn merge_threshold_fragmentation(&mut self, fragmentation: u8) -> &mut Self {
        self.merge.thresholds.fragmentation = fragmentation;
        self
    }

    /// Set the minimum amount of bytes occupied by dead keys that will cause a data file to be
    /// included during a merge. Default to `128MiBs`
    pub fn merge_threshold_dead_bytes(&mut self, dead_bytes: ByteSize) -> &mut Self {
        self.merge.thresholds.dead_bytes = dead_bytes;
        self
    }

    /// Set the minimum file size that will cause a data file to be included during a merge.
    /// Default to `10MiBs`
    pub fn merge_threshold_small_file(&mut self, small_file: ByteSize) -> &mut Self {
        self.merge.thresholds.small_file = small_file;
        self
    }

    /// Set the interval that Bitcask periodically runs checks to determine whether to merge.
    /// Default `3 minutes`.
    pub fn merge_check_interval(&mut self, check_interval: time::Duration) -> &mut Self {
        self.merge.check_inverval = check_interval;
        self
    }

    /// Set the integer percentage of the random variation applied to the merge interval.
    /// Default `30`.
    pub fn merge_check_jitter(&mut self, check_jitter: u8) -> &mut Self {
        self.merge.check_jitter = check_jitter;
        self
    }
}
