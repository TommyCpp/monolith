use crate::wal::WalErr::FileIoErr;
use crate::wal::{Entry, SyncCache, SyncPolicy, WalErr, WAL_MAGIC_NUMBER};
use crate::{MonolithErr, Result};
use futures::io::Error;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

type FileMutex = Arc<Mutex<File>>;

const AVG_NUM_BYTES_IN_ENTRY: usize = 32;

///!  Wal File structure
///! |  component  | length  |
///! |-------------|---------|
///! | Magic Number| u64     |
///! | Entries     | n * u8  |
///! | .......     | ....... |
///! | First seqid | u64     |
///! | Last seqid  | u64     |
///! | CRC64       | u64     |

/// SegmentWriter is writer for segment file
///
/// SegmentWriter is **NOT** concurrent-safe.
pub struct SegmentWriter {
    first_idx: u64,
    last_idx: u64,

    file: File,
    cache: SyncCache,

    crc: crc::crc64::Digest,
}

impl SegmentWriter {
    /// Create an segment file, open with append-only
    ///
    /// If there is no file yet. Create one and write the overhead.
    /// If there is one, update `first_idx` and `last_idx`, also verify the crc and reset the cursor
    /// so that we could override crc at the end of the file.
    pub fn new<P: AsRef<Path>>(sync_policy: SyncPolicy, path: P) -> Result<SegmentWriter> {
        let (first_idx, last_idx, crc_initial, mut file) = if path.as_ref().exists() {
            // if there is file
            let mut file = OpenOptions::new().read(true).append(true).open(&path)?;

            // check for crc
            // get crc value in file
            let pos = file.seek(SeekFrom::End(-8))?;
            let crc_val = file.read_be_u64()?;
            // read content
            file.seek(SeekFrom::Start(0));
            let mut buf = vec![0; pos as usize];
            file.read_exact(buf.as_mut_slice());
            if crc::crc64::checksum_ecma(buf.as_slice()) != crc_val {
                // corrupt file. return error
                return Err(MonolithErr::from(WalErr::FileIoErr(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "crc fail to match",
                ))));
            }

            // get first id and last id
            file.seek(SeekFrom::Current(-16));
            let first_id = file.read_be_u64()?;
            let last_id = file.read_be_u64()?;

            // reset cursor so that we could over write crc
            // we need to minus 16 because we also need to overwrite `first_index` and `last_index`
            // but they are part of the value to calculate CRC. So we cannot just do it before we verify
            // the crc.
            file.set_len(pos - 16);

            // re-calculate CRC. The old crc including `first_index` and `last_index`
            // So we need to remove that part and re-calculate
            let new_crc_val = crc::crc64::checksum_ecma(&buf.as_slice()[..buf.len() - 16]);

            Ok((first_id, last_id, new_crc_val, file))
        } else {
            let mut file = OpenOptions::new().append(true).create(true).open(&path)?;

            // write magic number. Note that we don't include this when compute CRC
            file.write(&WAL_MAGIC_NUMBER.to_be_bytes()[..]);
            let crc_val = crc::crc64::checksum_ecma(&WAL_MAGIC_NUMBER.to_be_bytes()[..]);
            file.sync_all();
            Ok((0, 0, crc_val, file))
        }
        .map_err(|err| WalErr::FileIoErr(err))?;

        Ok(SegmentWriter {
            first_idx,
            last_idx,
            file,
            cache: match sync_policy {
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
            crc: crc::crc64::Digest::new_with_initial(crc::crc64::ECMA, crc_initial),
        })
    }

    /// Sync all data and close the file
    pub fn close(&mut self) -> Result<()> {
        self.flush()?; // flush all cached entries

        // append first and last sequence id
        self.write(&self.first_idx.to_be_bytes())?;
        self.write(&self.last_idx.to_be_bytes())?;

        // append crc
        let _crc = self.crc.finish();
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
    /// Based on different sync policy. It will trigger sync to make sure data reaches disk.
    /// User can also manually call `sync` method to ask segment to sync immediately flush cache and
    /// sync disk.
    pub fn write_entry(&mut self, entry: Entry) -> Result<()> {
        if self.first_idx == 0 {
            self.first_idx = entry.seq_id;
        }
        if self.last_idx >= entry.seq_id {
            return Err(MonolithErr::WalErr(WalErr::InternalError(
                "sequence id is smaller than the last sequence id in writer".to_string(),
            )));
        }
        self.last_idx = entry.seq_id;

        // Update cache
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

impl Write for SegmentWriter {
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

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        self.close();
    }
}

/// SegmentReader if reader for segment file.
pub struct SegmentReader {
    first_idx: u64,
    last_idx: u64,
    file: File,
}

impl SegmentReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SegmentReader> {
        unimplemented!();
    }
}

trait ReadExt<T> {
    fn read_be_u64(&mut self) -> std::result::Result<u64, std::io::Error>;
}

impl<T> ReadExt<T> for T
where
    T: Read,
{
    fn read_be_u64(&mut self) -> std::result::Result<u64, std::io::Error> {
        let mut buffer = vec![0; 8];
        self.read_exact(buffer.as_mut_slice())?;

        Ok(u64::from_be_bytes(buffer.as_slice().try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::segment::SegmentWriter;
    use crate::wal::{Entry, EntryType, SyncPolicy, WAL_MAGIC_NUMBER};
    use crate::Result;
    use futures::io::SeekFrom;
    use std::fs;
    use std::fs::{File, OpenOptions};
    use std::hash::Hasher;
    use std::io::SeekFrom::Start;
    use std::io::{Read, Seek, Write};
    use tempfile::TempPath;

    #[test]
    pub fn test_write_entry() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let mut segment =
            SegmentWriter::new(SyncPolicy::Immediate, &temp_path.path().join("test1"))?;
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
    pub fn test_write_entry_with_num_cache() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let file_path = temp_path.path().join("test1");
        let mut segment = SegmentWriter::new(SyncPolicy::NumBased(5), &file_path)?;

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
        let mut segment = SegmentWriter::new(SyncPolicy::NumBased(20), &file_path)?;
        segment.write_entry(Entry::new(1u64, EntryType::Default, vec![]));

        assert_eq!(fs::read_to_string(&file_path)?.len(), 8); // no entry yet.

        segment.close();

        assert_eq!(fs::read_to_string(&file_path)?.len(), 8 + 15 + 8); // flushed one entry(15 bytes) and crc 64

        Ok(())
    }

    #[test]
    pub fn test_open_existing_log_file() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let file_path = temp_path.path().join("test1");
        let mut segment = SegmentWriter::new(SyncPolicy::Immediate, &file_path)?;
        assert_eq!(
            segment.crc.finish(),
            crc::crc64::checksum_ecma(&WAL_MAGIC_NUMBER.to_be_bytes()[..])
        );
        segment.write_entry(Entry::new(1, EntryType::Default, vec![]));
        segment.write_entry(Entry::new(2, EntryType::Default, vec![]));
        drop(segment); // close segment;

        let mut segment = SegmentWriter::new(SyncPolicy::Immediate, &file_path)?;
        assert_eq!(segment.first_idx, 1);
        assert_eq!(segment.last_idx, 2);

        segment.write_entry(Entry::new(3, EntryType::Default, vec![]));
        segment.write_entry(Entry::new(4, EntryType::Default, vec![]));
        drop(segment);

        let mut segment = SegmentWriter::new(SyncPolicy::Immediate, &file_path)?;
        assert_eq!(segment.first_idx, 1);
        assert_eq!(segment.last_idx, 4);

        Ok(())
    }
}
