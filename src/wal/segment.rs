use std::fs::File;
use crate::Result;
use std::io::Write;
use crate::wal::{FlushCache, WalErr, Entry};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

type FileMutex = Arc<Mutex<File>>;

/// Segment is one write ahead file
///
pub struct Segment {
    next_seq: AtomicU64,
    file: FileMutex,
    cache: FlushCache,
    crc: crc::crc64::Digest,
}

impl Write for Segment {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        unimplemented!();
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let remaining = if let &FlushCache::NumBase {
            ref cache,
            ..
        } = &self.cache {
            cache.clone()
        } else {
            vec![]
        };
        if remaining.len() > 0{
            for entry in remaining{
                self.write_entry(entry);
            }
        }

        match &mut self.cache {
            &mut FlushCache::None => {}
            &mut FlushCache::NumBase {
                ref size,
                ref mut idx,
                ref mut cache,
            } => {
                *idx = 0;
                *cache = vec![Entry::default(); *size];
            }
            &mut FlushCache::TimeBased {
                ..
            } => {
                // fixme and above
                ()
            }
        }
        match &mut self.file.lock() {
            Ok(ref mut file) => {
                file.flush()
            }
            Err(err) => {
                Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
            }
        }
    }
}

impl Segment {
    pub fn new() -> Result<Segment> {
        unimplemented!();
    }

    /// flush all data and close the file
    pub fn close(&mut self) -> Result<()> {
        unimplemented!()
    }

    pub fn write_entry(&mut self, entry: Entry) -> Result<()>{
        unimplemented!();
    }
}