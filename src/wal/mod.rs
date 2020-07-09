use std::path::PathBuf;
use std::sync::Mutex;

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
#[derive(Clone)]
pub struct Entry {
    seq_id: u64,
    entry_type: u8,
    content: Vec<u8>,
    // in bytes
    len: usize,
    crc: [u8; 4], // CRC 32
}

impl Default for Entry {
    fn default() -> Self {
        Entry {
            seq_id: 0,
            entry_type: 0,
            content: vec![],
            len: 0,
            crc: [0, 0, 0, 0],
        }
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

pub enum FlushCache {
    TimeBased {
        handler: std::thread::Thread,
        cache: Mutex<Vec<Entry>>,
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
    #[fail(display = "Write ahead log internal error {}", _0)]
    InternalError(String)
}

