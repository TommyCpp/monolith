use crate::wal::{Entry, SyncCache, SyncPolicy, WalErr, WAL_MAGIC_NUMBER};
use crate::Result;
use std::fs::File;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

type FileMutex = Arc<Mutex<File>>;

const AVG_NUM_BYTES_IN_ENTRY: usize = 32;

/// Segment is one write ahead file
///
/// Segment is **NOT** concurrent-safe.
pub struct Segment {
    last_idx: u64,
    file: File,
    cache: SyncCache,
    crc: crc::crc64::Digest,
}

impl Write for Segment {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.crc.write(bytes); // update crc
        self.file.write(bytes)
    }

    /// Flush all data into OS's file system cache.
    /// Note that there is no guarantee that it has been synced to disk.
    /// To make sure all data has been synced to disk. Use `Segment::sync()`
    fn flush(&mut self) -> std::io::Result<()> {
        let remaining: Vec<Entry> = match &mut self.cache {
            &mut SyncCache::None => vec![],
            &mut SyncCache::NumBased {
                limit: ref size,
                ref mut idx,
                ref mut cache,
            } => {
                // Note that here the cache have a fix size and we initial every element to be default
                // entry. So we need to get the actual meaningful entries. Thus, we only take the
                // first `idx` entries.
                // Also note that `idx` will always insert index of next entry.
                let mut _cache = Vec::<Entry>::with_capacity(*idx);
                _cache.extend_from_slice(&cache.as_slice()[0..*idx]);

                *idx = 0;
                *cache = vec![Entry::default(); *size];
                _cache
            }
            &mut SyncCache::TimeBased { .. } => {
                // fixme and above
                vec![]
            }
            &mut SyncCache::SizeBased {
                ref limit,
                ref mut cache,
                ref mut size,
            } => {
                *size = 0;
                let _cache = cache.clone();
                *cache = Vec::with_capacity(limit / AVG_NUM_BYTES_IN_ENTRY);
                _cache
            }
        };

        if remaining.len() > 0 {
            for entry in remaining {
                self.write(entry.get_bytes().as_slice())?;
            }
        }

        Ok(())
    }
}

impl Segment {
    pub fn new(flush_policy: SyncPolicy, mut file: File) -> Result<Segment> {
        // write magic number. Note that we don't include this when compute CRC
        file.write(&WAL_MAGIC_NUMBER.to_be_bytes()[..]);
        file.sync_all();

        Ok(Segment {
            last_idx: 0,
            file,
            cache: match flush_policy {
                SyncPolicy::Immediate => SyncCache::None,
                SyncPolicy::TimeBased(dur) => {
                    let cache = Arc::new(Mutex::new(vec![]));
                    SyncCache::TimeBased {
                        handler: std::thread::spawn(|| {}), //todo: figure out how to do time based flush
                        cache,
                    }
                }
                SyncPolicy::NumBased(limit) => SyncCache::NumBased {
                    limit,
                    idx: 0,
                    cache: vec![Entry::default(); limit],
                },
                SyncPolicy::SizeBased(limit) => SyncCache::SizeBased {
                    limit,
                    size: 0,
                    cache: Vec::with_capacity(limit * AVG_NUM_BYTES_IN_ENTRY),
                },
            },
            crc: crc::crc64::Digest::new(crc::crc64::ECMA),
        })
    }

    /// Sync all data and close the file
    pub fn close(&mut self) -> Result<()> {
        let _crc = self.crc.finish();
        self.flush()?;
        self.file.write(&_crc.to_be_bytes())?; // append crc result
        self.sync(); // make sure all data synced.
        Ok(())
    }

    /// Ensure all data has been written to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Write an entry into segment file.
    ///
    /// Based on different flush policy. It will trigger sync to make sure data reaches disk.
    pub fn write_entry(&mut self, entry: Entry) -> Result<()> {
        self.last_idx = entry.seq_id;
        match &mut self.cache {
            &mut SyncCache::None => {
                self.write(entry.get_bytes().as_slice())?;
                self.sync();
            }
            &mut SyncCache::NumBased {
                limit: size,
                ref mut idx,
                ref mut cache,
            } => {
                cache[*idx] = entry;
                *idx += 1;
                if *idx == size {
                    self.sync();
                }
            }
            &mut SyncCache::TimeBased { ref mut cache, .. } => {
                cache.lock().unwrap().push(entry);
            }
            &mut SyncCache::SizeBased {
                ref mut cache,
                ref limit,
                ref mut size,
            } => {
                let entry_len = entry.len();
                cache.push(entry);
                if *size + entry_len >= *limit {
                    self.sync();
                } else {
                    *size += entry_len;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::segment::Segment;
    use crate::wal::{Entry, EntryType, SyncPolicy, WAL_MAGIC_NUMBER};
    use crate::Result;
    use futures::io::SeekFrom;
    use std::fs;
    use std::fs::{File, OpenOptions};
    use std::io::SeekFrom::Start;
    use std::io::{Read, Seek, Write};
    use tempfile::TempPath;

    #[test]
    pub fn test_write_entry() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let mut temp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(temp_path.path().join("test1"))?;
        let mut segment = Segment::new(SyncPolicy::Immediate, temp_file)?;
        let mut entry = Entry::new(1u64, EntryType::Default, vec![]);
        entry.push(vec![1, 2, 3, 4]);
        segment.write_entry(entry)?;

        // Verify
        let mut result_file = OpenOptions::new()
            .read(true)
            .open(temp_path.path().join("test1"))?;

        // check for magic num
        let mut magic_num = [0u8; 8];
        result_file.seek(SeekFrom::Start(0)).unwrap();
        result_file.read(&mut magic_num);
        assert_eq!(magic_num, WAL_MAGIC_NUMBER.to_be_bytes());

        // check for sequence id
        let mut seq_id = [0u8; 8];
        result_file.read(&mut seq_id);
        assert_eq!(seq_id, 1u64.to_be_bytes());

        // check for entry type
        let mut entry_type = [0u8; 1];
        result_file.read(&mut entry_type);
        assert_eq!(entry_type, [0u8]);

        // check for length
        let mut len = [0u8; 2];
        result_file.read(&mut len);
        assert_eq!(len, [0u8, 4u8]);

        // check for content
        let mut content = vec![0u8; 4];
        result_file.read(content.as_mut_slice());
        assert_eq!(content, vec![1, 2, 3, 4]);

        // check for checksum
        let mut checksum = vec![0u8; 4];
        result_file.read(&mut checksum);
        assert_eq!(checksum, vec![182u8, 60, 251, 205]);

        Ok(())
    }

    #[test]
    pub fn test_write_entry_with_cache() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let file_path = temp_path.path().join("test1");
        let mut temp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;
        let mut segment = Segment::new(SyncPolicy::NumBased(5), temp_file)?;

        for i in 1..5 {
            segment.write_entry(Entry::new(i as u64, EntryType::Default, vec![]));
        }

        // no entry yet.
        assert_eq!(fs::read_to_string(&file_path)?.len(), 8);

        segment.write_entry(Entry::new(5u64, EntryType::Default, vec![]));

        // Assert we have flush 5 entries.
        assert_eq!(fs::read_to_string(&file_path)?.len(), 8 + 15 * 5);

        segment.write_entry(Entry::new(6u64, EntryType::Default, vec![]));
        segment.flush(); // force flush
        assert_eq!(fs::read_to_string(&file_path)?.len(), 8 + 15 * 6);

        Ok(())
    }

    #[test]
    pub fn test_close_entry() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let file_path = temp_path.path().join("test1");
        let mut temp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;
        let mut segment = Segment::new(SyncPolicy::NumBased(20), temp_file)?;
        segment.write_entry(Entry::new(1u64, EntryType::Default, vec![]));

        assert_eq!(fs::read_to_string(&file_path)?.len(), 8); // no entry yet.

        segment.close();

        assert_eq!(fs::read_to_string(&file_path)?.len(), 8 + 15 + 8); // flushed one entry(15 bytes) and crc 64

        Ok(())
    }
}
