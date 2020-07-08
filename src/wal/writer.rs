use std::sync::Mutex;
use std::fs::File;
use std::io::Write;

pub struct WalWriter {
    next_seq: AtomicU64,
    mux: Mutex<File>,
    crc: crc::crc64::Digest
}

impl Write for WalWriter{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}