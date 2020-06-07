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
    // last leading zero
    leading: u8,
    // last tailing zero
    trailing: u8,
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
            leading: 0,
            trailing: 0,
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
            } else if in_bit_range(dod, 8) {
                self.bstream.append_bytes(&vec![0b10000000], 6);
                self.bstream.write_bits(dod.to_be_bytes(), 8)
            } else if in_bit_range(dod, 16) {
                self.bstream.append_bytes(&vec![0b11000000], 5);
                self.bstream.write_bits(dod.to_be_bytes(), 16)
            } else if in_bit_range(dod, 32) {
                self.bstream.append_bytes(&vec![0b11100000], 4);
                self.bstream.write_bits(dod.to_be_bytes(), 32);
            } else {
                self.bstream.append_bytes(&vec![0b11110000], 4);
                self.bstream.write_bits(dod.to_be_bytes(), 64);
            }
            self.compact_value(&tp.value);
        }
        self.len += 1;
        self.t = tp.timestamp;
        self.v = tp.value;
    }

    pub fn compact_value(&mut self, value: &Value) {
        if self.len == 0 {
            // if it's the first value, skip XOR, added it to the current bstream
            self.bstream.append_bytes(&value.to_be_bytes().to_vec(), 0);
            // Update value in self
            self.v = *value;
            self.leading = convert_to_u64(self.v.clone()).leading_zeros() as u8;
            self.trailing = convert_to_u64(self.v.clone()).trailing_zeros() as u8;
        } else {
            let xord: u64 = convert_to_u64(*value) ^ convert_to_u64(self.v.clone());
            if xord == 0 {
                // single zero bit
                self.bstream.write_zero();
            } else {
                // single one bit
                self.bstream.write_one();
                let leading: u8 = xord.leading_zeros() as u8;
                let trailing: u8 = xord.trailing_zeros() as u8;
                if leading >= self.leading && trailing >= self.trailing {
                    self.bstream.write_zero();
                    self.bstream.write_bits((xord >> self.trailing as u64).to_be_bytes(), 64 - self.leading - self.trailing);
                } else {
                    self.trailing = trailing;
                    self.leading = leading;

                    self.bstream.write_one();
                    // Use the next 5 bits to store len of leading zeros
                    self.bstream.write_bits((leading as u64).to_be_bytes(), 5);

                    let sigbits = 64 - leading - trailing;
                    self.bstream.write_bits((sigbits as u64).to_be_bytes(), 6);
                    self.bstream.write_bits((xord >> trailing as u64).to_be_bytes(), sigbits);
                }
            }
        }
    }
}

fn convert_to_u64(v: f64) -> u64 {
    u64::from_be_bytes(v.to_be_bytes())
}


// Find if the timestamp's meaningful in the `nbit` range.
fn in_bit_range(t: i64, nbits: u8) -> bool {
    return -((1 << (nbits - 1)) - 1) as i64 <= t && t <= 1 << (nbits - 1) as i64;
}
