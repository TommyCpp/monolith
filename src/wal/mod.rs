use std::path::PathBuf;
use std::sync::{Mutex, Arc};
use crc::Hasher32;

mod segment;
mod writer;

/// Note that we only use the first three byte in this magic number
/// User can still use the remaining 5 bytes to add more metadata
/// Like WAL for different component
const WAL_MAGIC_NUMBER: u64 = 0x57414C0000000000u64;

// Wal File structure
// |  component  | length  |
// |-------------|---------|
// | Magic Number| u64     |
// | Entries     | n * u64 |
// | .......     | ....... |
// | CRC64       | u64     |


/// Entry encoding
/// -----------------------------------------------------------------
/// | seq_num(u64) | type(u8) | len(u16) | data(bytes) | CRC32(u32) |
/// -----------------------------------------------------------------
pub struct Entry {
    seq_id: u64,
    entry_type: u8,
    content: Vec<u8>,
    crc: crc::crc32::Digest, // CRC 32
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Entry {
            seq_id: self.seq_id,
            entry_type: self.entry_type,
            content: self.content.clone(),
            crc: crc::crc32::Digest::new_with_initial(crc::crc32::IEEE, self.crc.sum32()),
        }
    }
}

impl Default for Entry {
    fn default() -> Self {
        Entry {
            seq_id: 0,
            entry_type: 0,
            content: vec![],
            crc: crc::crc32::Digest::new(crc::crc32::IEEE),
        }
    }
}

impl Entry {
    pub fn get_bytes(&self) -> Vec<u8> {
        let _crc = self.crc.sum32();
        let mut _res = Vec::<u8>::from(&self.seq_id.to_be_bytes()[..]);
        _res.push(self.entry_type);
        _res.append(self.content.clone().as_mut());
        _res.extend_from_slice(&_crc.to_be_bytes()[..]);

        _res
    }

    pub fn push(&mut self, mut content: Vec<u8>) {
        self.crc.write(content.as_slice());
        self.content.append(content.as_mut())
    }
}

pub struct WalConfig {
    pub filepath: PathBuf
}

pub enum FlushPolicy {
    TimeBased(std::time::Duration),
    NumBased(usize),
    // flush whenever insert data.
    Immediate,
}

/// FlushCache tells segment how to cache bytes
pub enum FlushCache {
    TimeBased {
        handler: std::thread::JoinHandle<()>,
        cache: Arc<Mutex<Vec<Entry>>>,
    },
    NumBase {
        size: usize,
        idx: usize,
        cache: Vec<Entry>,
    },
    None,
}


#[derive(Debug, Fail)]
pub enum WalErr {
    #[fail(display = "Internal error {}", _0)]
    InternalError(String),

    #[fail(display = "{}", _0)]
    FileIoErr(std::io::Error),
}

#[cfg(test)]
mod tests {
    use crate::wal::Entry;
    use crc::Hasher32;

    #[test]
    pub fn test_entry_get_bytes() {
        let mut entry = Entry::default();
        entry.push(vec![1, 2, 3, 4]);
        let bytes = entry.get_bytes();
        let sum32 = crc::crc32::checksum_ieee(vec![1u8, 2, 3, 4].as_slice());
        let mut result = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4];
        result.extend_from_slice(&sum32.to_be_bytes()[..]);
        assert_eq!(bytes, result)
    }

    #[test]
    pub fn test_entry_clone() {
        let mut entry = Entry::default();
        entry.push(vec![1, 3, 4, 5]);

        let mut entry_clone = entry.clone();

        assert_eq!(entry_clone.crc.sum32(), entry.crc.sum32());
    }
}