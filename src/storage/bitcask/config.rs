use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::Bitcask;

/// Configuration for a `Bitcask` instance. We try to mirror the configurations
/// available in [Configuring Bitcask].
///
/// [Configuring Bitcask]: https://docs.riak.com/riak/kv/latest/setup/planning/backend/bitcask/index.html#configuring-bitcask
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Path to the storage data directory.
    pub path: PathBuf,

    pub(super) concurrency: usize,
    pub(super) max_file_size: u64,
    pub(super) sync: SyncStrategy,
    pub(super) merge: MergeStrategy,
}

/// Control how data is synchronized to disk.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStrategy {
    /// Data is written to disk when the operating system flushes its buffers.
    None,
    /// Force a synchronization after every write.
    Always,
    /// Synchronize the the file system at the specified interval.
    IntervalMs(u64),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MergeStrategy {
    pub policy: MergePolicy,
    pub triggers: MergeTriggers,
    pub thresholds: MergeThresholds,
    pub check_interval_ms: u64,
    pub check_jitter: f64,
}

/// Control how data files are merged.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergePolicy {
    Always,
    Never,
    Window { start: u32, end: u32 },
}

/// List of conditions that trigger the data files merging process
#[derive(Debug, Clone, Deserialize)]
pub struct MergeTriggers {
    pub fragmentation: f64,
    pub dead_bytes: u64,
}

/// List of conditions that trigger the data files merging process
#[derive(Debug, Clone, Deserialize)]
pub struct MergeThresholds {
    pub fragmentation: f64,
    pub dead_bytes: u64,
    pub small_file: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: std::env::current_dir().unwrap(),
            concurrency: num_cpus::get(),
            max_file_size: 2 * 1024 * 1024 * 1024,
            sync: SyncStrategy::default(),
            merge: MergeStrategy::default(),
        }
    }
}

impl Default for SyncStrategy {
    fn default() -> Self {
        Self::None
    }
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self {
            check_interval_ms: 18000,
            check_jitter: 0.3,
            policy: MergePolicy::default(),
            triggers: MergeTriggers::default(),
            thresholds: MergeThresholds::default(),
        }
    }
}
impl Default for MergePolicy {
    fn default() -> Self {
        Self::Always
    }
}
impl Default for MergeTriggers {
    fn default() -> Self {
        Self {
            fragmentation: 0.6,
            dead_bytes: 512 * 1024 * 1024,
        }
    }
}
impl Default for MergeThresholds {
    fn default() -> Self {
        Self {
            fragmentation: 0.4,
            dead_bytes: 128 * 1024 * 1024,
            small_file: 10 * 1024 * 1024,
        }
    }
}

impl Config {
    /// Create a `Bitcask` instance at the given path with the available options.
    pub fn open(self) -> Result<Bitcask, super::Error> {
        Bitcask::open(self)
    }

    /// Set path to the storage directory. Default to the current directory.
    pub fn path<P>(&mut self, path: P) -> &mut Self
    where
        P: AsRef<Path>,
    {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the max number of concurrent readers. Default to the number of logical cores.
    pub fn concurrency(&mut self, concurrency: usize) -> &mut Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the max file size in byte. Default to `2GiBs`.
    pub fn max_file_size(&mut self, max_file_size: u64) -> &mut Self {
        self.max_file_size = max_file_size;
        self
    }

    /// Set the synchronization strategy. Default to `SyncStrategy::None`.
    pub fn sync(&mut self, sync: SyncStrategy) -> &mut Self {
        self.sync = sync;
        self
    }

    /// Set the merge policy. Default to `MergePolicy::Always`.
    pub fn merge_policy(&mut self, policy: MergePolicy) -> &mut Self {
        if let MergePolicy::Window { start, end } = policy {
            assert!((0..24).contains(&start));
            assert!((0..24).contains(&end));
        }
        self.merge.policy = policy;
        self
    }

    /// Set the fraction of dead keys to total keys that will trigger a merge (min 0.0, max 1.0).
    /// Default to `0.6`.
    ///
    /// # Panics
    ///
    /// If the given fraction is not in [0, 1] then panics
    pub fn merge_trigger_fragmentation(&mut self, fragmentation: f64) -> &mut Self {
        assert!((0.0..=1.0).contains(&fragmentation));
        self.merge.triggers.fragmentation = fragmentation;
        self
    }

    /// Set the minimum amount of bytes occupied by dead keys that will trigger a merge.
    /// Default to `512MiBs`.
    pub fn merge_trigger_dead_bytes(&mut self, dead_bytes: u64) -> &mut Self {
        self.merge.triggers.dead_bytes = dead_bytes;
        self
    }

    /// Set the fraction of dead keys to total keys that will cause a data file to be
    /// included during a merge (min 0.0, max 1.0). Default to `0.4`.
    ///
    /// # Panics
    ///
    /// If the given fraction is not in [0, 1] then panics
    pub fn merge_threshold_fragmentation(&mut self, fragmentation: f64) -> &mut Self {
        assert!((0.0..=1.0).contains(&fragmentation));
        self.merge.thresholds.fragmentation = fragmentation;
        self
    }

    /// Set the minimum amount of bytes occupied by dead keys that will cause a data file to be
    /// included during a merge. Default to `128MiBs`
    pub fn merge_threshold_dead_bytes(&mut self, dead_bytes: u64) -> &mut Self {
        self.merge.thresholds.dead_bytes = dead_bytes;
        self
    }

    /// Set the minimum file size that will cause a data file to be included during a merge.
    /// Default to `10MiBs`
    pub fn merge_threshold_small_file(&mut self, small_file: u64) -> &mut Self {
        self.merge.thresholds.small_file = small_file;
        self
    }

    /// Set the interval in millisecond that Bitcask periodically runs checks to determine whether to merge.
    /// Default `3 minutes`.
    pub fn merge_check_interval_ms(&mut self, check_interval_ms: u64) -> &mut Self {
        self.merge.check_interval_ms = check_interval_ms;
        self
    }

    /// Set the fraction of the random variation applied to the merge interval (min 0.0, max 1.0)
    /// Default `0.3`.
    ///
    /// # Panics
    ///
    /// If the given fraction is not in [0, 1] then panics
    pub fn merge_check_jitter(&mut self, check_jitter: f64) -> &mut Self {
        assert!((0.0..=1.0).contains(&check_jitter));
        self.merge.check_jitter = check_jitter;
        self
    }
}
