use crate::wal::SyncPolicy;
use crate::{
    wal::{segment::Segment, Entry, EntryType},
    Result,
};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

type LockedSegment = Arc<RwLock<Segment>>;

pub struct WalConfig {
    sync_policy: SyncPolicy,
    initial_seq: u64,
}

/// Wal manages a dict of segment wal files.
///
pub struct Wal<'a> {
    dir: &'a Path,
    flush_policy: SyncPolicy,

    // generate the file name to maintain file order.
    next_seq: AtomicU64,
    // active segment
    active_seg: LockedSegment,
    // read only ordered in creating time. From earliest to latest
    segments: Vec<Arc<Segment>>,
}

impl<'a> Default for Wal<'a> {
    fn default() -> Self {
        unimplemented!()
    }
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
