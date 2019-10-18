use std::collections::BTreeMap;

use crate::label::*;
use crate::time_point::*;
use std::num::TryFromIntError;
use std::sync::atomic::{AtomicU64, Ordering};

pub type TimeSeriesId = u64;


pub struct TimeSeries {
    id: TimeSeriesId,
    time_points: Vec<TimePoint>,
    meta_data: Labels,
}

impl TimeSeries {
    pub fn new(id: TimeSeriesId, meta_data: Labels) -> TimeSeries {
        TimeSeries {
            id,
            time_points: Vec::new(),
            meta_data,
        }
    }

    pub fn meta_date(self) -> Labels {
        self.meta_data
    }

    pub fn add(&mut self, timestamp: Timestamp, value: Value) {
        self.time_points.push(TimePoint::new(timestamp, value))
    }
}


pub struct IdGenerator(AtomicU64);

impl IdGenerator {
    pub fn new(init_id: TimeSeriesId) -> IdGenerator {
        IdGenerator(AtomicU64::new(init_id))
    }
    pub fn next(&self) -> TimeSeriesId {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::label::Labels;
    use crate::time_point::*;
    use crate::time_series::{TimeSeries, IdGenerator};

    #[test]
    fn crate_time_series() {
        let time_series = TimeSeries::new(12, Labels::new());
    }

    #[test]
    fn generate_id() {
        let id_generator = IdGenerator::new(2);
        assert_eq!(id_generator.next(), 2);
        assert_eq!(id_generator.next(), 3)
    }
}