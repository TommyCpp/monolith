use crate::common::time_point::TimePoint;
use crate::compaction::{Bstream, BstreamSeeker};
use crate::{Timestamp, Value};
use std::convert::TryInto;

pub fn compact(data: Vec<TimePoint>) -> Vec<u8> {
    unimplemented!()
}

pub struct GorillaCompactor {
    bstream: Bstream,
    // last timestamp
    t: Timestamp,
    // last value
    v: Value,
    // last leading zero
    leading: u8,
    // last tailing zero
    tailing: u8,
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
            tailing: 0,
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
            // Because it's enough to cover the four hour interval and the time window in Gorilla is two.
            // second comporessed timestamp should be delta between the first timestamp in time stream and the aligned start time
            // Again, in Prometheus, Instead of using 14 bits to store. They use the delta between first and second timestamp as the second compressed timestamp
            // Again, we follow prometheus' idea
            let t_delta = (tp.timestamp - self.t) as i64;
            self.bstream.append_bytes(&t_delta.to_be_bytes(), 0);
            self.compact_value(&tp.value);

            self.d = t_delta;
        } else {
            let t_delta: i64 = (tp.timestamp - self.t) as i64;
            let dod = t_delta - self.d;

            self.d = t_delta;

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
        let value_size = std::mem::size_of::<Value>() * 8;
        if self.len == 0 {
            // if it's the first value, skip XOR, added it to the current bstream
            self.bstream.append_bytes(&value.to_be_bytes().to_vec(), 0);
            // Update value in self
            self.v = *value;
            self.leading = convert_to_u64(self.v.clone()).leading_zeros() as u8;
            self.tailing = convert_to_u64(self.v.clone()).trailing_zeros() as u8;
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
                if self.len > 1 && leading >= self.leading && trailing >= self.tailing {
                    self.bstream.write_zero(); // write 10
                    self.bstream.write_bits((xord >> self.tailing as u64).to_be_bytes(), value_size as u8 - self.leading - self.tailing);
                } else {
                    self.tailing = trailing;
                    self.leading = leading;

                    self.bstream.write_one(); // write 11
                    // Use the next 5 bits to store len of leading zeros
                    self.bstream.write_bits((leading as u64).to_be_bytes(), 5);

                    let sigbits = value_size as u8 - leading - trailing;
                    self.bstream.write_bits((sigbits as u64).to_be_bytes(), 6);
                    self.bstream.write_bits((xord >> trailing as u64).to_be_bytes(), sigbits);
                }
            }
        }
    }
}

pub struct GorillaDecompactor {
    source: BstreamSeeker,
    // current timestamp
    t: Timestamp,
    // current value
    v: Value,
    // current leading zero
    leading: u8,
    // current tailing zero
    tailing: u8,
    // currently delta
    d: i64,
    // timepoint index
    idx: usize,
}

impl GorillaDecompactor {
    pub fn new(bstream: Bstream) -> GorillaDecompactor {
        GorillaDecompactor {
            source: BstreamSeeker::new(bstream),
            t: 0,
            v: 0.0,
            leading: 0,
            tailing: 0,
            d: 0,
            idx: 0,
        }
    }

    fn decompact_values(&mut self) -> Option<Value> {
        let value_size = std::mem::size_of::<Value>() * 8;

        // Decompact the values
        let mut next_bytes = vec![0x00];
        let mut value = self.v;
        let rl = self.source.read_next_n_bit(next_bytes.as_mut_slice(), 2);
        if rl > 2 || rl == 0 {
            return None;
        }
        if rl == 1 || next_bytes[0] & 0b10000000 == 0 {
            // if the xord value equals to 0, reset cursor back 1 bits
            // if we only get 1 bit, then we don't really need to return any bits.
            self.source.reset_cursor(self.source.get_cursor() + (2 - rl) - 1);
        } else if next_bytes == vec![0x10] {
            // if the xord flag equals to 10, then
            // First get the len of value
            let len = (value_size as u8 - self.leading - self.tailing) as usize;
            let mut bytes = vec_with_len(value_size / 8);
            let l = self.source.read_next_n_bit(bytes.as_mut_slice(), len);
            if l != len {
                return None;
            }
            let value = convert_to_f64(u64::from_be_bytes(bytes.as_slice().try_into().unwrap()) << self.tailing);
            self.v = value;

            return Some(value);
        } else {
            // if the xord flag equals to 11, then we use get next 5 bits to find leading zeros
            let mut leading_zeros_bytes = vec![0x00];
            let l = self.source.read_next_n_bit(leading_zeros_bytes.as_mut_slice(), 5);
            if l != 5 {
                return None;
            }
            let leading = u8::from_be_bytes((leading_zeros_bytes[0] >> 3).to_be_bytes());

            let mut sigbits_len = vec![0x00];
            let l = self.source.read_next_n_bit(sigbits_len.as_mut_slice(), 6);
            if l != 6 {
                return None;
            }
            let sig_len = u8::from_be_bytes((sigbits_len[0] >> 2).to_be_bytes());

            let tailing = value_size as u8 - sig_len - leading;

            let mut sigbit = vec_with_len(value_size / 8);
            let l = self.source.read_next_n_bit(sigbit.as_mut_slice(), sig_len as usize);
            if l != sig_len as usize {
                return None;
            }
            // We need to left move `tailing` bits because we right moved when compacted/
            let xord = convert_to_f64(u64::from_be_bytes(sigbit.as_slice().try_into().unwrap()) << tailing);
            value = xor_f64(value, xord);


            self.tailing = tailing;
            self.leading = leading;
            self.v = value;
        }
        return Some(value);
    }

    fn decompact_timestamp(&mut self, range: usize) -> Option<Timestamp> {
        if !vec![8, 16, 32, 64].contains(&range) {
            return None;
        }

        let mut bytes = vec_with_len(range / 8);
        let l = self.source.read_next_n_bit(bytes.as_mut_slice(), range);
        if l != range {
            return None;
        }
        match range {
            8 => {
                self.d = i8::from_be_bytes(bytes.as_slice().try_into().unwrap()) as i64 + self.d;
            }
            16 => {
                self.d = i16::from_be_bytes(bytes.as_slice().try_into().unwrap()) as i64 + self.d;
            }
            32 => {
                self.d = i32::from_be_bytes(bytes.as_slice().try_into().unwrap()) as i64 + self.d;
            }
            64 => {
                self.d = i64::from_be_bytes(bytes.as_slice().try_into().unwrap()) + self.d;
            }
            _ => {
                // should never been here.
            }
        }
        let timestamp = self.t + self.d as u64;
        Some(timestamp)
    }
}

impl Iterator for GorillaDecompactor {
    type Item = TimePoint;

    fn next(&mut self) -> Option<Self::Item> {
        let timestamp_size = std::mem::size_of::<Timestamp>() * 8;
        let value_size = std::mem::size_of::<Value>() * 8;

        return if self.idx == 0 {
            // if first, we read the first 128 bit and convert them into (timestamps, values)
            let mut timepoint = vec_with_len((timestamp_size + value_size) / 8);
            let l = self.source.read_next_n_bit(timepoint.as_mut_slice(), timestamp_size + value_size);
            if l != timestamp_size + value_size {
                // if we fail to get the first 128, most likely we have a rouge data stream, abort
                None
            } else {
                let (timestamp_bytes, value_bytes) = timepoint.split_at(timestamp_size / 8);
                let timestamp = Timestamp::from_be_bytes(timestamp_bytes.try_into().unwrap());
                let value = Value::from_be_bytes(value_bytes.try_into().unwrap());

                self.t = timestamp;
                self.v = value;
                self.idx += 1;

                Some(TimePoint::new(timestamp, value))
            }
        } else if self.idx == 1 {
            // If second, we need to extract the delta
            let mut delta_v = vec_with_len(timestamp_size / 8);
            let l = self.source.read_next_n_bit(delta_v.as_mut_slice(), timestamp_size);
            if l != timestamp_size {
                return None;
            }

            let delta = i64::from_be_bytes(delta_v.as_slice().try_into().unwrap());
            let timestamp = delta + self.t as i64;

            self.d = delta;
            self.t = timestamp as u64;

            if let Some(value) = self.decompact_values() {
                self.idx += 1;
                Some(TimePoint::new(timestamp as u64, value))
            } else {
                None
            }
        } else {
            let mut dod_flag_bytes = vec![0x00]; // bytes of len of delta of delta
            let l = self.source.read_next_n_bit(dod_flag_bytes.as_mut_slice(), 4);
            if l == 0 || l > 4 {
                return None;
            }
            let dod_flag_byte = dod_flag_bytes[0];
            let mut timestamp = 0;
            if dod_flag_byte & 0b10000000 == 0 {
                // dod == 0
                timestamp = self.t + self.d as u64;
                self.source.reset_cursor(self.source.get_cursor() + (4 - l) - 3);
            } else if dod_flag_byte & 0b11000000 == 0b10000000 {
                // dod is in next 8 bits
                self.source.reset_cursor(self.source.get_cursor() + (4 - l) - 2);
                if let Some(ts) = self.decompact_timestamp(8) {
                    timestamp = ts;
                } else {
                    return None;
                }
            } else if dod_flag_byte & 0b11100000 == 0b11000000 {
                // dod is in next 16 bits
                self.source.reset_cursor(self.source.get_cursor() + (4 - l) - 1);
                if let Some(ts) = self.decompact_timestamp(16) {
                    timestamp = ts;
                } else {
                    return None;
                }
            } else if dod_flag_byte & 0b11110000 == 0b11100000 {
                // dod is in next 32 bits
                if let Some(ts) = self.decompact_timestamp(32) {
                    timestamp = ts;
                } else {
                    return None;
                }
            } else {
                if let Some(ts) = self.decompact_timestamp(64) {
                    timestamp = ts;
                } else {
                    return None;
                }
            }
            self.t = timestamp;
            if let Some(value) = self.decompact_values() {
                self.idx += 1;
                Some(TimePoint::new(timestamp, value))
            } else {
                None
            }
        };
    }
}

fn convert_to_u64(v: f64) -> u64 {
    u64::from_be_bytes(v.to_be_bytes())
}

fn convert_to_f64(v: u64) -> f64 {
    f64::from_be_bytes(v.to_be_bytes())
}

fn xor_f64(a: f64, b: f64) -> f64 {
    convert_to_f64(convert_to_u64(a) ^ convert_to_u64(b))
}

fn vec_with_len(len: usize) -> Vec<u8> {
    let mut v = vec![];
    for _ in 0..len {
        v.push(0x00u8)
    }
    v
}

// Find if the timestamp's meaningful in the `nbit` range.
fn in_bit_range(t: i64, nbits: u8) -> bool {
    return -((1 << (nbits - 1)) - 1) as i64 <= t && t <= 1 << (nbits - 1) as i64;
}


#[cfg(test)]
mod tests {
    use crate::compaction::{GorillaCompactor, BstreamSeeker, Bstream};
    use crate::common::time_point::TimePoint;
    use crate::compaction::gorilla::GorillaDecompactor;
    use futures::SinkExt;

    #[test]
    pub fn test_gorilla_compaction() {
        type Timestream = Vec<(u64, f64)>;
        let data: Vec<(Timestream, Vec<u8>, u8)> = vec![
            (vec![(128, 1.5), (129, 1.5), (130, 1.5), (131, 1.5), (132, 1.5)],
             vec![0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, // first timestamp
                  0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first value
                  0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01, // first delta, which is 1
                  0x0
             ],
             1
            ),
            (vec![(100, 1.5), (102, 1.5), (105, 1.5), (106, 1.5)],
             vec![0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, // first timestamp
                  0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first value
                  0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x02, // first delta, which is 2
                  0x40, 0x2b, 0xf8
                  // How it works
                  // First append 64 bit timestamp and 64 bit values
                  // Then append delta between second timestamp and first timestamp, which is 1
                  // Then append second value, whose xored value is 0, thus we append one bit 0
                  // Now bstream looks like
                  // 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, [first timestamp]
                  // 0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, [first value]
                  // 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x02, [first delta, which is 2]
                  // 0b0[second value]
                  // Then we start the third timestamp, we have delta of delta 3-2 = 1
                  // So we put 10 and 2 as i8, which change the last byte into
                  // 0b0[second value]1000000\001[third timestamp]
                  // Then we append 0 since the third value is not changing.
                  // 0b0[second value]1000000\001[third timestamp]0[third value]
                  // Then we process the fourth timestamp, we have delta of delta 1-3 = -2
                  // Thus we have last few bytes
                  // 0b0[second value]1000000\001[third timestamp]0[third value]1011\111110[forth timestamp]
                  // Now we append last 0 as the value is still not changing
                  // as result
                  // 0b0[second value]1000000\001[third timestamp]0[third value]1011\111110[forth timestamp]0[last value]0[not used]
             ],
             1
            ),
            (
                vec![(128, 1.5), (129, 1.6)],
                vec![
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, // first timestamp
                    0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first value
                    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01, // first delta, which is 1
                    0xdf, 0x86, 0x66, 0x66, 0x66, 0x66, 0x66, 0x68
                    // How it works
                    // First append 128 bit timestamp and values
                    // Then append delta between second and first timestamp as above.
                    // Now bstream looks like
                    // 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, [first timestamp]
                    // 0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, [first value]
                    // 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x02, [first delta, which is 1]
                    //
                    // Now we deal with second value.
                    // The first value is
                    // 00111111, 11111000, 00000000, 00000000, 00000000, 00000000, 00000000, 00000000
                    // The second value is
                    // 00111111, 11111001, 10011001, 10011001, 10011001, 10011001, 10011001, 10011010
                    // The xord value is
                    // 00000000, 00000001, 10011001, 10011001, 10011001, 10011001, 10011001, 10011010
                    // leading zero is 8 + 7 = 15, tailing zero is 1
                    // Since we haven't have any leading and tailing zero in record, we store the leading and tailing zero.
                    //
                    // Append flag 0b11
                    // Append 0b01111[leading zero, 15]\110000[significant bit num, 64-15-1=48]
                    // Append 48 bit of significant bit, which is 110011001100110011001100110011001100110011001101

                    // Now the we append value of second value into bit stream
                    // 0b1101111[leading zero, 15]\110000[significant bit num]\110011001100110011001100110011001100110011001101[significant bit]
                    // List above value as bytes
                    // 11011111
                    // 10000110
                    // 01100110
                    // 01100110
                    // 01100110
                    // 01100110
                    // 01100110
                    // 01101

                    // todo: add more time points
                ],
                3
            )
        ];

        for (input, output, remaining) in data {
            let mut compactor = GorillaCompactor::new();
            for (t, v) in input {
                compactor.compact(&TimePoint::new(t, v));
            }
            assert_eq!(compactor.bstream.data, output);
            assert_eq!(compactor.bstream.remaining, remaining);
        }
    }

    #[test]
    pub fn test_gorilla_decompaction() {
        type Timestream = Vec<(u64, f64)>;
        let data: Vec<(Vec<u8>, u8, Timestream)> = vec![
            (vec![0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, // first timestamp
                  0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first value
                  0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01, // first delta, which is 1
                  0x0
            ],
             1,
             vec![(128, 1.5), (129, 1.5), (130, 1.5), (131, 1.5), (132, 1.5)]
            ),
            (vec![0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, // first timestamp
                  0x3f, 0xf8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first value
                  0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x02, // first delta, which is 1
                  0x40, 0x2b, 0xf8],
             1,
             vec![(100, 1.5), (102, 1.5), (105, 1.5), (106, 1.5)]
            ),
        ];

        for (i_data, i_remain, tstream) in data {
            let decompactor = GorillaDecompactor::new(Bstream {
                data: i_data,
                remaining: i_remain,
            });
            let res: Timestream = decompactor.into_iter()
                .map(|tp| (tp.timestamp, tp.value)).collect::<Timestream>();
            assert_eq!(res, tstream);
        }
    }
}