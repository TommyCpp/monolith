use std::fs::File;
use crate::Result;
use std::io::Write;
use crate::wal::{FlushCache, WalErr, Entry, FlushPolicy};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::hash::Hasher;

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
        self.crc.write(bytes); // update crc

        match self.file.lock().as_mut() {
            Ok(ref mut file) => {
                file.write(bytes)
            }
            Err(err) => {
                Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
            }
        }
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
        if remaining.len() > 0 {
            for entry in remaining {
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
    pub fn new(flush_policy: FlushPolicy, file: File) -> Result<Segment> {
        let file_mux = Arc::new(Mutex::new((file)));
        Ok(
            Segment {
                next_seq: std::sync::atomic::AtomicU64::new(1u64),
                file: file_mux,
                cache: match flush_policy {
                    FlushPolicy::Immediate => FlushCache::None,
                    FlushPolicy::TimeBased(dur) => {
                        let cache = Arc::new(Mutex::new(vec![]));
                        FlushCache::TimeBased {
                            handler: std::thread::spawn(|| {
                            }),  //todo: figure out how to do time based flush
                            cache,
                        }
                    }
                    FlushPolicy::NumBased(size) => {
                        FlushCache::NumBase {
                            size,
                            idx: 0,
                            cache: Vec::with_capacity(size),
                        }
                    }
                },
                crc: crc::crc64::Digest::new(crc::crc64::ECMA),
            }
        )
    }

    /// flush all data and close the file
    pub fn close(&mut self) -> Result<()> {
        let _crc = self.crc.finish();
        self.write(&_crc.to_be_bytes())?; // append crc result
        Ok(())
    }

    pub fn write_entry(&mut self, entry: Entry) -> Result<()> {
        match &mut self.cache {
            &mut FlushCache::None => {
                self.write(entry.get_bytes().as_slice())?;
            }
            &mut FlushCache::NumBase {
                ref size,
                ref mut idx,
                ref mut cache,
            } => {
                cache[*idx] = entry;
                *idx += 1;
                if idx == size {
                    self.flush();
                }
            }
            &mut FlushCache::TimeBased {
                ref mut cache, ..
            } => {
                cache.lock().unwrap().push(entry);
            }
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {}