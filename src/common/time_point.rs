use std::collections::BTreeMap;
use std::time::Duration;

pub const TIME_UNIT: Duration = Duration::from_nanos(1);


pub type Timestamp = u64;
pub type Value = f64;


pub struct TimePoint {
    timestamp: Timestamp,
    value: Value,

//    How do we organize the metadata
}

impl TimePoint {
    pub fn new(timestamp: Timestamp, value: Value) -> TimePoint {
        TimePoint { timestamp, value }
    }
}

