use crate::common::time_point::TimePoint;
use crate::compaction::Bstream;
use crate::{Timestamp, Value};

pub fn compact(data: Vec<TimePoint>) -> Vec<u8> {
    unimplemented!()
}

struct GorillaCompactor {
    bstream: Bstream,
    // last timestamp
    t: Timestamp,
    // last value
    v: Value,
    // last delta
    d: i64,
    len: usize,
}

impl GorillaCompactor {
    pub fn new() -> GorillaCompactor {
        GorillaCompactor {
            bstream: Bstream::new(),
            d: 0,
            v: 0.0,
            t: 0,
            len: 0,
        }
    }

    // Reference https://github.com/prometheus/prometheus/blob/master/tsdb/chunkenc/xor.go#L150
    pub fn compact(&mut self, tp: &TimePoint) {
        if self.len == 0 {
            // if 0, means we haven't have any bit, initialize by appending ts
            // Gorilla's starting time uses a two-hours windows.
            // The first timestamp will be aligned with nearest two hour start time.
            // The Prometheus implementation uses first timestamp as the start
            // Our implementation follows the same way as Prometheus
            self.bstream.append_bytes(&tp.timestamp.to_be_bytes(), 0);
            self.bstream.append_bytes(&tp.value.to_be_bytes(), 0);
        } else if self.len == 1 {
            // In Gorilla's implementation, the second compressed timestamp should be 14 bits
            // second comporessed timestamp should be delta between the first timestamp in time stream and the aligned start time
            // Because it's enough to cover the four hour interval and the time window in Gorilla is two.
            // Again, in Prometheus, Instead of using 14 bits to store. They use the delta between first and second timestamp as the second compressed timestamp
            // Again, we follow prometheus' idea
            let t_delta = (tp.timestamp - self.t) as i64;
            self.bstream.append_bytes(&t_delta.to_be_bytes(), 0);
            self.compact_value(&tp.value);

            self.d = t_delta;
        } else {
            let t_delta: i64 = (tp.timestamp - self.t) as i64;
            let dod = t_delta - self.d;
            if dod == 0 {
                self.bstream.append_bytes(&vec![0b00000000], 7);
            } else if in_bit_range(dod, 14) {
                self.bstream.append_bytes(&vec![0b10000000], 6);
                self.bstream.append_timestamp_delta(dod, 14)
            } else if in_bit_range(dod, 17) {
                self.bstream.append_bytes(&vec![0b11000000], 5);
                self.bstream.append_timestamp_delta(dod, 17)
            } else if in_bit_range(dod, 20) {
                self.bstream.append_bytes(&vec![0b11100000], 4);
                self.bstream.append_timestamp_delta(dod, 20);
            } else {
                self.bstream.append_bytes(&vec![0b11110000], 4);
                self.bstream.append_timestamp_delta(dod, 64);
            }
            self.compact_value(&tp.value);
        }
        self.len += 1;
        self.t = tp.timestamp;
        self.v = tp.value;
    }

    pub fn compact_value(&mut self, value: &Value) {
        unimplemented!();
    }
}


// Find if the timestamp's meaningful in the `nbit` range.
fn in_bit_range(t: i64, nbits: u8) -> bool {
    return -((1 << (nbits - 1)) - 1) as i64 <= t && t <= 1 << (nbits - 1) as i64;
}