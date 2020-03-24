use failure::_core::cmp::Ordering;

use std::time::Duration;
use crate::proto::Sample;

pub const TIME_UNIT: Duration = Duration::from_micros(1);
pub const F64_MARGIN: f64 = 0.000000001;

pub type Timestamp = u64;
pub type Value = f64;

#[derive(Clone, Debug)]
pub struct TimePoint {
    pub timestamp: Timestamp,
    pub value: Value,
    //    How do we organize the metadata
}

impl TimePoint {
    pub fn new(timestamp: Timestamp, value: Value) -> TimePoint {
        TimePoint { timestamp, value }
    }
}

impl Eq for TimePoint {}

impl PartialEq for TimePoint {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp.eq(&other.timestamp)
            && (self.value - other.value < F64_MARGIN || other.value - self.value < F64_MARGIN)
    }
}

impl Ord for TimePoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for TimePoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl From<&TimePoint> for crate::proto::Sample {
    fn from(t: &TimePoint) -> Self {
        Sample {
            timestamp: t.timestamp as i64,
            value: t.value,
            unknown_fields: Default::default(),
            cached_size: Default::default()
        }
    }
}

impl From<&crate::proto::Sample> for TimePoint{
    fn from(s: &Sample) -> Self {
        TimePoint{
            timestamp: s.timestamp as Timestamp,
            value: s.value
        }
    }
}

#[cfg(test)]
mod test {
    use crate::common::time_point::TimePoint;

    #[test]
    fn create_timepoint() {
        let timepoint = TimePoint::new(120, 12.0);
        assert_eq!(timepoint.timestamp, 120);
        assert_eq!(timepoint.value, 12.0);
    }
}
