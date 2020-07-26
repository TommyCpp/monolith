use crate::wal::{Entry, FlushCache, FlushPolicy, WalErr, WAL_MAGIC_NUMBER};
use crate::Result;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

type FileMutex = Arc<Mutex<File>>;

const AVG_NUM_BYTES_IN_ENTRY: usize = 8;

/// Segment is one write ahead file
///
/// Segment is **NOT** concurrent-safe.
pub struct Segment<W: Write> {
    writer: W,
    cache: FlushCache,
    crc: crc::crc64::Digest,
}

impl<W: Write> Write for Segment<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.crc.write(bytes); // update crc
        self.writer.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let remaining = match &mut self.cache {
            &mut FlushCache::None => vec![],
            &mut FlushCache::NumBased {
                limit: ref size,
                ref mut idx,
                ref mut cache,
            } => {
                *idx = 0;
                let _cache = cache.clone();
                *cache = vec![Entry::default(); *size];
                _cache
            }
            &mut FlushCache::TimeBased { .. } => {
                // fixme and above
                vec![]
            }
            &mut FlushCache::SizeBased {
                ref limit,
                ref mut cache,
                ref mut size,
            } => {
                *size = 0;
                let _cache = cache.clone();
                *cache = Vec::with_capacity(limit * AVG_NUM_BYTES_IN_ENTRY);
                _cache
            }
        };

        if remaining.len() > 0 {
            for entry in remaining {
                self.write(entry.content.as_slice())?;
            }
        }

        self.writer.flush()
    }
}

impl<W> Segment<W>
    where
        W: Write,
{
    pub fn new(flush_policy: FlushPolicy, mut writer: W) -> Result<Segment<W>> {
        // write magic number. Note that we don't include this when compute CRC
        writer.write(&WAL_MAGIC_NUMBER.to_be_bytes()[..]);
        writer.flush();

        Ok(Segment {
            writer,
            cache: match flush_policy {
                FlushPolicy::Immediate => FlushCache::None,
                FlushPolicy::TimeBased(dur) => {
                    let cache = Arc::new(Mutex::new(vec![]));
                    FlushCache::TimeBased {
                        handler: std::thread::spawn(|| {}), //todo: figure out how to do time based flush
                        cache,
                    }
                }
                FlushPolicy::NumBased(limit) => FlushCache::NumBased {
                    limit,
                    idx: 0,
                    cache: Vec::with_capacity(limit),
                },
                FlushPolicy::SizeBased(limit) => FlushCache::SizeBased {
                    limit,
                    size: 0,
                    cache: Vec::with_capacity(limit * AVG_NUM_BYTES_IN_ENTRY),
                },
            },
            crc: crc::crc64::Digest::new(crc::crc64::ECMA),
        })
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
                self.flush();
            }
            &mut FlushCache::NumBased {
                limit: ref size,
                ref mut idx,
                ref mut cache,
            } => {
                cache[*idx] = entry;
                *idx += 1;
                if idx == size {
                    self.flush();
                }
            }
            &mut FlushCache::TimeBased { ref mut cache, .. } => {
                cache.lock().unwrap().push(entry);
            }
            &mut FlushCache::SizeBased {
                ref mut cache,
                ref limit,
                ref mut size,
            } => {
                let entry_len = entry.len();
                cache.push(entry);
                if *size + entry_len >= *limit {
                    self.flush();
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
    use crate::wal::{Entry, EntryType, FlushPolicy, WAL_MAGIC_NUMBER};
    use crate::Result;
    use futures::io::SeekFrom;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek};
    use tempfile::TempPath;

    #[test]
    pub fn test_write_entry() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let mut temp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(temp_path.path().join("test1"))?;
        let mut segment = Segment::<File>::new(FlushPolicy::Immediate, temp_file)?;
        let mut entry = Entry::new(1u64, EntryType::Default);
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
}
