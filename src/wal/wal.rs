use crate::wal::SyncPolicy;
use crate::{
    wal::{segment::SegmentWriter, Entry, EntryType},
    Result,
};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

type LockedSegment = Arc<RwLock<SegmentWriter>>;

pub struct WalConfig {
    sync_policy: SyncPolicy,
    // must be larger than 1.
    initial_seq: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            sync_policy: SyncPolicy::Immediate,
            initial_seq: 1,
        }
    }
}

/// Wal manages a dictionary of segment wal files.
///
/// Wal is concurrent-safe
///
/// Wal will
/// 1. Manage and assign sequence id.
/// 2. Rotate segment when its size is too large.
/// 3. Clean up segment file once user know the change has been written.
pub struct Wal<'a> {
    dir: &'a Path,
    sync_policy: SyncPolicy, // sync policy of segment.

    // generate the file name to maintain file order.
    next_seq: AtomicU64,
    // active segment
    active_seg: LockedSegment,
    // read only ordered in creating time. From earliest to latest
    segments: Vec<Arc<SegmentWriter>>,
}

impl<'a> Wal<'a> {
    /// Open an dictionary to read and write write ahead log.
    /// If there are already some segment file in dictionary. Read them.
    fn open<P: AsRef<Path>>(path: P, config: Option<WalConfig>) -> Result<Wal<'a>> {
        unimplemented!();
    }

    /// Write an entry into wal.
    /// Not necessary to sync immediately.
    fn write(&self, entry_type: EntryType, data: &[u8]) -> Result<()> {
        unimplemented!();
    }

    /// Force flush to make sure all contents in cache will be written to storage
    fn flush(&self) -> Result<()> {
        unimplemented!()
    }

    /// Ready one entry with entry id `idx`
    fn read(&self, idx: u64) -> Result<Entry> {
        unimplemented!();
    }
}
