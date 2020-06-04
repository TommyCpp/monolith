#![allow(dead_code)]

use crate::common::time_point::TimePoint;
use std::ops::Deref;
use std::io::{BufReader, Read, Write};
use std::io;
use crate::Timestamp;

mod simple;
mod gorilla;

// Compressor is used to compress the timestamp and values when the chunk goes to closed
pub enum Compactor {
    // Facebook's tsdb
    // See https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
    // Implementation Reference https://github.com/prometheus/prometheus/blob/master/tsdb/chunkenc
    Gorilla,

    // Simple version of Gorilla
    // instead of change in bits, we use bytes.
    Simple,
}

impl Compactor {
    // Compress a stream of <timestamp, value> pairs
    pub fn compact(&self, data: Vec<TimePoint>) -> Vec<u8> {
        match self {
            &Compactor::Simple => {
                vec![]
            }
            &Compactor::Gorilla => unimplemented!(),
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
struct Bstream {
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

    /// Create Bstream from one byte
    pub fn from_bytes(data: Vec<u8>, remaining: u8) -> Bstream {
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

                for i in 1..bstream.data.len() {
                    let mut first = bstream.data.get(i - 1).unwrap().clone();
                    let mut second = bstream.data.get(i).unwrap().clone();
                    first <<= self.remaining;
                    second >>= (8 - self.remaining);
                    first |= second;
                    self.data.push(first);
                }
                if self.remaining <= (8 - bstream.remaining) && bstream.data.len() > 1 {
                    // in this case, the last part in bstream is not empty, we need to add it
                    // if there is only one element in bstream.data, we already added it.
                    self.data.push(
                        bstream.data.last().unwrap().clone() << self.remaining);
                    self.remaining = bstream.remaining + self.remaining;
                } else {
                    self.remaining = bstream.remaining - self.remaining;
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

    // append delta of timestamps, only put last `bits` bit into bstream
    pub fn append_timestamp_delta(&mut self, delta: i64, bits: u8){
        unimplemented!();
    }
}


#[cfg(test)]
mod tests {
    use crate::compaction::Bstream;

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
            let mut bstream1 = Bstream::from_bytes(d1, r1);
            let mut bstream2 = Bstream::from_bytes(d2, r2);
            bstream1.append(&bstream2);
            assert_eq!(expect_data, bstream1.data);
            assert_eq!(expect_remaining, bstream1.remaining)
        }
    }
}