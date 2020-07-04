#![allow(dead_code)]

use crate::common::time_point::TimePoint;
use std::ops::Deref;
use std::io::{BufReader, Read, Write, Seek};
use std::io;
use std::fs::File;

use crate::{Result, MonolithErr};
use crate::Timestamp;

mod simple;
mod gorilla;

pub use gorilla::GorillaCompactor;
use crate::compaction::gorilla::GorillaDecompactor;
use crate::compaction::CompactionErr::CompactionTypeDontMatch;

pub enum CompactType {
    Gorilla = 1,
    Simple = 2,
    None = 3,
}

// Compressor is used to compress the timestamp and values when the chunk goes to closed
pub enum Compactor {
    // Facebook's tsdb
    // See https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
    // Implementation Reference https://github.com/prometheus/prometheus/blob/master/tsdb/chunkenc
    Gorilla(GorillaCompactor),

    // Simple version of Gorilla
    // instead of change in bits, we use bytes.
    // todo: implement it
    Simple,

    // No compaction
    None(Vec<u8>),
}

impl Compactor {
    pub fn new(name: CompactType) -> Compactor {
        match name {
            CompactType::Gorilla => {
                Compactor::Gorilla(GorillaCompactor::new())
            }
            CompactType::None => {
                Compactor::None(vec![])
            }
            CompactType::Simple => {
                Compactor::Simple
            }
        }
    }

    /// Compact a single time point
    pub fn compact(&mut self, timepoint: &TimePoint) {
        match self {
            &mut Compactor::Gorilla(ref mut compator) => {
                compator.compact(timepoint);
            }
            &mut Compactor::None(ref mut _v) => {
                for _b in &timepoint.timestamp.to_be_bytes() {
                    _v.push(*_b);
                }
                for _b in &timepoint.value.to_be_bytes() {
                    _v.push(*_b);
                }
            }
            _ => {}
        }
    }

    /// Compact a stream of <timestamp, value> pairs
    /// ```
    /// use monolith::Compactor;
    /// use monolith::time_point::TimePoint;
    /// use monolith::CompactType;
    ///
    /// fn main() {
    ///     let tp = TimePoint::new(1, 1.0);
    ///     let mut compactor = Compactor::new(CompactType::Gorilla);
    ///     let res = compactor.compact_vec(vec![tp]);
    /// }
    /// ```
    pub fn compact_vec(&mut self, data: Vec<TimePoint>) -> Vec<u8> {
        match self {
            &mut Compactor::Simple => {
                vec![]
            }
            &mut Compactor::Gorilla(ref mut compator) => {
                for tp in data {
                    compator.compact(&tp);
                }
                let mut vec = compator.as_bytes_vec();

                // Append Gorilla tag
                vec.push(CompactType::Gorilla as u8);
                vec
            }
            &mut Compactor::None(ref _v) => {
                let mut vec = _v.clone();
                vec.push(CompactType::None as u8);
                vec
            }
        }
    }
}

pub enum DeCompactor {
    Gorilla(GorillaDecompactor),
    None(Vec<u8>)
}

impl DeCompactor {
    pub fn from(t: CompactType, mut data: Vec<u8>) -> Result<DeCompactor> {
        match t {
            CompactType::Gorilla => {
                let t = data.pop().unwrap_or(0);
                if t != CompactType::Gorilla as u8 {
                    return Err(MonolithErr::CompactionErr(CompactionErr::CompactionTypeDontMatch(CompactType::Gorilla as u8, t)));
                }
                let bstream = Bstream::from_bytes(data);
                let decompactor = GorillaDecompactor::new(bstream);
                Ok(
                    DeCompactor::Gorilla(decompactor)
                )
            }
            CompactType::None => {
                let t = data.pop().unwrap_or(0);
                if t != CompactType::None as u8{
                    return Err(
                        MonolithErr::CompactionErr(CompactionErr::CompactionTypeDontMatch(CompactType::None as u8, t))
                    );
                }
                Ok(
                    DeCompactor::None(data)
                )
            },
            _ => Err(MonolithErr::InternalErr("Not implemented".into())),
        }
    }
}


/// Stream of bit data, used to read and write bit data.
///
/// Bstream is not thread safe.
///
/// We must filling one byte before appending another one to data.
/// The `remaining` indicates how may bit is available in current byte.
///
pub struct Bstream {
    data: Vec<u8>,
    remaining: u8, // the available bit in last byte
}

impl Bstream {
    pub fn new() -> Bstream {
        Bstream {
            data: vec![],
            remaining: 0,
        }
    }

    /// Create Bstream from vector of bytes and remaining
    pub fn from(data: Vec<u8>, remaining: u8) -> Bstream {
        Bstream {
            data,
            remaining,
        }
    }

    /// Create Bstream from data, the remaining will be stored as last byte in u8 vectors.
    ///
    /// Note the difference with `Bstream::from`
    pub fn from_bytes(mut data: Vec<u8>) -> Bstream {
        let remaining = u8::from_be_bytes([data.pop().unwrap_or(0u8)]);
        Bstream {
            data,
            remaining,
        }
    }

    /// Write a single bit to the end of bit stream
    pub fn write(&mut self, bit: bool) {
        if self.remaining == 0 {
            // if there is no available bit
            if bit {
                self.data.push(0b10000000)
            } else {
                self.data.push(0b00000000)
            }
            self.remaining = 7
        } else {
            self.remaining -= 1;
            if bit {
                *(self.data.last_mut().unwrap()) |= (0b00000001 << self.remaining);
            }
        }
    }

    /// How many bit in this bit stream
    pub fn bitlen(&self) -> usize {
        self.data.len() * 8 - self.remaining as usize
    }

    /// append one of the bit stream to another
    pub fn append(&mut self, bstream: &Bstream) {
        let bstream_len = bstream.bitlen();
        if bstream_len == 0 {
            return;
        }

        // first fill what's available in current vector.
        if self.remaining != 0 {
            if bstream_len <= self.remaining as usize {
                // if we have enough space left in current
                *(self.data.last_mut().unwrap())
                    |= bstream.data.get(0).unwrap().clone() >> (8 - self.remaining);
                self.remaining -= bstream_len as u8;
            } else {
                // Get the first part of the bits of bstream, which will be appended into current one.
                let mut mask = bstream.data.get(0).unwrap().clone();
                mask >>= (8 - self.remaining);
                *(self.data.last_mut().unwrap()) |= mask;

                for i in 0..(bstream.data.len() - 1) {
                    let mut first = bstream.data.get(i).unwrap().clone();
                    let mut second = bstream.data.get(i + 1).unwrap().clone();
                    first <<= self.remaining;
                    second >>= (8 - self.remaining);
                    first |= second;
                    self.data.push(first);
                }
                if self.remaining <= (8 - bstream.remaining) {
                    // in this case, the last part in bstream is not empty, we need to add it
                    // if there is only one element in bstream.data, we already added it.
                    self.data.push(
                        bstream.data.last().unwrap().clone() << self.remaining);
                    self.remaining = bstream.remaining + self.remaining;
                } else {
                    self.remaining = (bstream.remaining as i8 - self.remaining as i8).abs() as u8;
                }
            }
        } else {
            // if we have no space left, just append the bstream and use its remaining.
            self.data.append(bstream.data.clone().as_mut());
            self.remaining = bstream.remaining as u8;
        }
    }

    pub fn append_bytes(&mut self, bytes: &[u8], remaining: u8) {
        assert!(remaining <= 8 && remaining >= 0);
        self.append(&mut Bstream {
            data: Vec::from(bytes),
            remaining,
        })
    }

    // append delta of timestamps
    // Here only `bits` bit is meaningful.
    //
    // For example, if bits is 7, it means the delta is within [-63, 64] and we only need put 7 bit into bstream
    // value allowed here is [8, 16, 32, 64]
    pub fn append_timestamp_delta(&mut self, delta: i64, bits: u8) {
        match bits {
            8 => {
                let d = delta as i8;
                self.append_bytes(&d.to_be_bytes(), 0);
            }
            16 => {
                let d = delta as i16;
                self.append_bytes(&d.to_be_bytes(), 0);
            }
            32 => {
                let d = delta as i32;
                self.append_bytes(&d.to_be_bytes(), 0);
            }
            64 => {
                let d = delta as i64;
                self.append_bytes(&d.to_be_bytes(), 0);
            }
            _ => {}
        }
    }


    // Write bits into bstream.
    // Note that here we will write from left and removing the leading value
    // For example append_bits(0b00001010, 3) will write 010 into bstream.
    pub fn write_bits(&mut self, bytes: [u8; 8], nbits: u8) {
        let leading = 64 - nbits;
        let val = bytes[usize::from(leading) / 8] << leading % 8;
        self.append_bytes(&[val], leading % 8);
        self.append_bytes(&bytes[(usize::from(leading) / 8 + 1)..8], 0);
    }

    pub fn write_one(&mut self) {
        self.write(true);
    }

    pub fn write_zero(&mut self) {
        self.write(false);
    }

    /// Read bytes with byte index `idx`. If target byte is the last byte, return `remaining` as second return value. Otherwise, set second return value to be 0
    pub fn read_bytes(&self, idx: usize) -> Option<(u8, usize)> {
        if idx >= self.data.len() {
            None
        } else if idx == self.data.len() - 1 {
            Some((self.data.last().unwrap().clone(), self.remaining as usize))
        } else {
            Some((self.data.get(idx).unwrap().clone(), 0))
        }
    }

    /// Return underlying data with remaining encoded as last byte in byte vector
    ///
    /// Note that here we create a new byte vector instead of using data directly.
    ///
    /// Thus, changes made to `data` may not reflect in returned reference.
    pub fn as_bytes_vec(&self) -> Vec<u8> {
        let mut res = self.data.clone();
        res.push(self.remaining.to_be_bytes()[0]);
        res
    }
}

struct BstreamSeeker {
    data: Bstream,
    cursor: usize,
}

impl BstreamSeeker {
    pub fn new(data: Bstream) -> BstreamSeeker {
        BstreamSeeker {
            data,
            cursor: 0,
        }
    }

    pub fn new_with_cursor(data: Bstream, cursor: usize) -> BstreamSeeker {
        BstreamSeeker {
            data,
            cursor,
        }
    }


    /// Read the next n bits from bstream
    pub fn read_next_n_bit(&mut self, data: &mut [u8], n: usize) -> usize {
        if self.cursor > self.data.bitlen() {
            // if we already reach the end of the stream
            0
        } else {
            // determine how many bits we actually need to put into data
            let n = if self.cursor + n > self.data.bitlen() {
                self.data.bitlen() - self.cursor
            } else {
                n
            };
            // fill data
            let leading: u8 = (self.cursor % 8) as u8;
            let mut cur = 0;
            while cur * 8 < n {
                let (mut first, _) = self.data.read_bytes(self.cursor / 8 + cur).unwrap();
                first &= 0xff >> leading; // get last `8 - leading` bits
                first <<= leading; // move them to left

                let (mut second, _) = self.data.read_bytes(self.cursor / 8 + cur + 1)
                    .unwrap_or((0x00u8, self.data.remaining as usize));
                first |= second.checked_shr((8 - leading).into()).unwrap_or(0); // get first `leading` bits

                data[cur] = first;
                cur += 1;
            }

            // Reset last few bit that should be zero
            if cur != 0 {
                let mut flag = 0xffu8;
                if n % 8 != 0 {
                    // deal with overflow
                    flag <<= (8 - n % 8) as u8;
                }
                data[cur - 1] = flag & data[cur - 1];
            }


            self.cursor += n;
            n
        }
    }

    /// Set the cursor
    pub fn reset_cursor(&mut self, pos: usize) {
        self.cursor = pos;
    }

    /// Get current cursor
    pub fn get_cursor(&self) -> usize {
        self.cursor
    }
}

#[derive(Debug, Fail)]
pub enum CompactionErr {
    #[fail(display = "Error when compaction or de-compaction")]
    CompactionTypeDontMatch(u8, u8)
}

#[cfg(test)]
mod tests {
    use crate::compaction::{Bstream, BstreamSeeker};
    use tempfile::{TempPath, TempDir};
    use std::fs::File;
    use std::fs;
    use std::io::{Write, Read};

    #[test]
    pub fn test_write_bstream() {
        let mut bstream = Bstream::new();
        bstream.write(true);
        assert_eq!(0b10000000, *(bstream.data.first().unwrap()));
        bstream.write(false);
        assert_eq!(0b10000000, *(bstream.data.first().unwrap()));
        assert_eq!(6, bstream.remaining);
        bstream.write(true);
        assert_eq!(0b10100000, *(bstream.data.first().unwrap()));
    }

    #[test]
    pub fn test_append_bstream() {
        let data: Vec<(Vec<u8>, u8, Vec<u8>, u8, Vec<u8>, u8)> = vec![
            (vec![0b10010010, 0b10000000], 7, vec![0b10000000], 7, vec![0b10010010, 0b11000000], 6),
            (vec![0b10000000], 0, vec![0b00000000], 0, vec![0b10000000, 0b00000000], 0),
            (vec![0b00000000], 4, vec![0b11110000, 0b10000000], 7, vec![0b00001111, 0b00001000], 3),
            (vec![0b00000000], 4, vec![0b11110000, 0b10000100], 2, vec![0b00001111, 0b00001000, 0b01000000], 6)
        ];

        for (d1, r1, d2, r2, expect_data, expect_remaining) in data {
            let mut bstream1 = Bstream::from(d1, r1);
            let mut bstream2 = Bstream::from(d2, r2);
            bstream1.append(&bstream2);
            assert_eq!(expect_data, bstream1.data);
            assert_eq!(expect_remaining, bstream1.remaining)
        }
    }

    #[test]
    pub fn test_write_bits() {
        type InitialState = (Vec<u8>, u8);
        type Input = ([u8; 8], u8);
        type Result = (Vec<u8>, u8);
        let data: Vec<(InitialState, Input, Result)> = vec![
            ((vec![], 0),
             (8u64.to_be_bytes(), 4),
             (vec![0b10000000], 4)
            ),
            ((vec![], 0), (33u64.to_be_bytes(), 6), (vec![0b10000100], 2)),
            ((vec![0x02, 64], 5),
             (1u64.to_be_bytes(), 8),
             (vec![0x02, 64, 32], 5)
            )
        ];

        for (
            (data, remaining),
            (v, l),
            (res_d, res_r)
        ) in data {
            let mut bstream = Bstream {
                data,
                remaining,
            };
            bstream.write_bits(v, l);
            assert_eq!(bstream.data, res_d);
            assert_eq!(bstream.remaining, res_r);
        }
    }

    #[test]
    pub fn test_read_next_n_bits() {
        type InitialState = (Vec<u8>, u8, u8); // current bytes, remaining, cursor index
        type Input = u8;
        type Result = (Vec<u8>, usize);

        let data: Vec<(InitialState, Input, Result)> = vec![
            (
                (vec![0b01010101, 0b01111111, 0b11000000], 6, 1),
                16,
                (vec![0b10101010, 0b11111111], 16)
            ),
            (
                (vec![0b01111111, 0b11111111, 0b10101010], 0, 10),
                16,
                (vec![0b11111110, 0b10101000], 14)
            ),
            (
                (vec![0b01101101, 0b10110010, 0b10111110], 1, 10),
                16,
                (vec![0b11001010, 0b11111000], 13)
            ),
            (
                (vec![0b01011111, 0b11111111, 0b01100000], 0, 5),
                11,
                (vec![0b11111111, 0b11100000], 11)
            ),
            (
                (vec![0b01100010, 0b01011111], 0, 7),
                6,
                (vec![0b00101100], 6)
            )
        ];

        for (
            (data, remaining, cursor),
            n_bits,
            (res_data, res_remaining)
        ) in data {
            let mut seeker = BstreamSeeker {
                data: Bstream {
                    data,
                    remaining,
                },
                cursor: cursor as usize,
            };

            let mut v = vec![];
            for _ in 0..res_data.len() {
                v.push(0x00)
            }
            let u = seeker.read_next_n_bit(v.as_mut_slice(), n_bits as usize);
            assert_eq!(v, res_data);
            assert_eq!(u, res_remaining);
        }
    }

    #[test]
    pub fn test_bstream_serialization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_file");
        let mut temp_file = File::create(path.as_path()).unwrap();
        let bstream = Bstream::from(vec![0x11, 0x22, 0x33, 0x40], 3);

        // write to file
        temp_file.write(&bstream.as_bytes_vec());
        temp_file.flush();

        // read from file
        let data = fs::read(path.as_path()).unwrap();
        let read_bstream = Bstream::from_bytes(data);
        assert_eq!(bstream.data, vec![0x11, 0x22, 0x33, 0x40]);
        assert_eq!(bstream.remaining, 3);
    }
}