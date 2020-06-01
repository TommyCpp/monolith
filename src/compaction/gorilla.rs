use crate::common::time_point::TimePoint;
use crate::compaction::Bstream;
use crate::Timestamp;

pub fn compact(data: Vec<TimePoint>) -> Vec<u8> {
    unimplemented!()
}

struct GorillaCompactor {
    bstream: Bstream,
    last_delta: i64,
}

impl GorillaCompactor {
    pub fn new() -> GorillaCompactor {
        GorillaCompactor {
            bstream: Bstream::new(),
            last_delta: 0,
        }
    }

    // Reference https://github.com/prometheus/prometheus/blob/master/tsdb/chunkenc/xor.go#L150
    pub fn compact_timestamp(&mut self, ts: Timestamp) {
        //todo: figure out how to count how many time points has been compacted.
        let length = self.bstream.bitlen();
        if length == 0 {
            // if 0, means we haven't have any bit, initialize by appending ts
            // Gorilla's starting time uses a two-hours windows.
            self.bstream.append_bytes(&ts.to_be_bytes(), 0);
        }
        unimplemented!()
    }
}