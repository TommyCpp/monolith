use std::sync::Mutex;
use std::fs::File;
use std::path::PathBuf;


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
    len: usize,
    content: Vec<u8>,
    crc: [u8; 4], // CRC 32
}

pub struct WalConfig {
    pub filepath: PathBuf
}

