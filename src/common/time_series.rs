



use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp, Value};

pub type TimeSeriesId = u64;

pub struct TimeSeries {
    id: TimeSeriesId,
    time_points: Vec<TimePoint>,
    meta_data: Labels,
}

impl TimeSeries {
    pub fn new(id: TimeSeriesId, meta_data: Labels) -> Self {
        TimeSeries {
            id,
            time_points: Vec::new(),
            meta_data,
        }
    }

    pub fn meta_data(&self) -> &Labels {
        &self.meta_data
    }

    pub fn add(&mut self, timestamp: Timestamp, value: Value) {
        self.time_points.push(TimePoint::new(timestamp, value))
    }
}

#[cfg(test)]
mod test {
    use crate::common::label::Labels;
    use crate::common::time_series::TimeSeries;

    #[test]
    fn crate_time_series() {
        let _time_series = TimeSeries::new(12, Labels::new());
    }
}
