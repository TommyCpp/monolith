use std::collections::BTreeMap;

use crate::label::*;
use crate::time_point::*;

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

    pub fn add(&mut self, timestamp: Timestamp, value: Value) {
        self.time_points.push(TimePoint::new(timestamp, value))
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::label::Labels;
    use crate::time_point::*;
    use crate::time_series::TimeSeries;

    #[test]
    fn crate_time_series() {
        let time_series = TimeSeries::new(12, Labels::new());
    }
}