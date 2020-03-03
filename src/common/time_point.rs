use std::collections::BTreeMap;
use std::time::Duration;

pub const TIME_UNIT: Duration = Duration::from_nanos(1);


pub type Timestamp = u64;
pub type Value = f64;


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

#[cfg(test)]
mod test{
    use crate::common::time_point::TimePoint;

    #[test]
    fn create_timepoint() {
        let timepoint = TimePoint::new(120, 12.0);
        assert_eq!(timepoint.timestamp, 120);
        assert_eq!(timepoint.value, 12.0);
    }
}

