#![allow(dead_code)]

use crate::common::time_point::TimePoint;

mod simple;

// Compressor is used to compress the timestamp and values when the chunk goes to closed
pub enum Compactor {
    // Facebook's tsdb
    // See https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
    Gorilla,

    // Simple version of Gorilla
    // instead of change in bits, we use bytes.
    Simple
}

impl Compactor {
    // Compress a stream of <timestamp, value> pairs
    pub fn compact(&self, data: Vec<TimePoint>) -> Vec<u8> {
        match self {
            &Compactor::Simple => {
                vec![]
            },
            &Compactor::Gorilla => unimplemented!(),
        }
    }
}