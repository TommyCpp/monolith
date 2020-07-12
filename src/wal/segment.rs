use std::fs::File;
use crate::Result;
use std::io::Write;
use crate::wal::{FlushCache, WalErr, Entry, FlushPolicy, WAL_MAGIC_NUMBER};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::hash::Hasher;

type FileMutex = Arc<Mutex<File>>;

/// Segment is one write ahead file
///
pub struct Segment<W: Write> {
    next_seq: AtomicU64,
    writer: Arc<Mutex<W>>,
    cache: FlushCache,
    crc: crc::crc64::Digest,
}

impl<W: Write> Write for Segment<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.crc.write(bytes); // update crc

        match self.writer.lock().as_mut() {
            Ok(ref mut writer) => {
                writer.write(bytes)
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
        match &mut self.writer.lock() {
            Ok(ref mut file) => {
                file.flush()
            }
            Err(err) => {
                Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
            }
        }
    }
}

impl<W> Segment<W>
    where W: Write {
    pub fn new(flush_policy: FlushPolicy, mut writer: W) -> Result<Segment<W>> {
        // write magic number. Note that we don't include this when compute CRC
        writer.write(&WAL_MAGIC_NUMBER.to_be_bytes()[..]);
        writer.flush();

        let writer_mux = Arc::new(Mutex::new((writer)));
        Ok(
            Segment {
                next_seq: std::sync::atomic::AtomicU64::new(1u64),
                writer: writer_mux,
                cache: match flush_policy {
                    FlushPolicy::Immediate => FlushCache::None,
                    FlushPolicy::TimeBased(dur) => {
                        let cache = Arc::new(Mutex::new(vec![]));
                        FlushCache::TimeBased {
                            handler: std::thread::spawn(|| {}),  //todo: figure out how to do time based flush
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
                self.flush();
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
mod tests {
    use tempfile::TempPath;
    use std::fs::{OpenOptions, File};
    use crate::Result;
    use crate::wal::segment::Segment;
    use crate::wal::{FlushPolicy, Entry, EntryType, WAL_MAGIC_NUMBER};
    use std::io::{Seek, Read};
    use futures::io::SeekFrom;

    #[test]
    pub fn test_write_entry() -> Result<()> {
        let temp_path = tempfile::tempdir()?;
        let mut temp_file =
            OpenOptions::new().append(true).create(true).open(temp_path.path().join("test1"))?;
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