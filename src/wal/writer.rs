use std::sync::Mutex;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use crate::wal::segment::Segment;

pub struct WalWriter {
    next_seq: AtomicU64, // generate the file name to maintain file order.
    active_seg: Segment, // active segment
    segments: Vec<Segment>, // cannot write again
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