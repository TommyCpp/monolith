use std::collections::BTreeMap;

pub type Timestamp = f64;
pub type Value = f64;

pub struct TimePoint {
    timestamp: Timestamp,
    value: Value,
    metadata: BTreeMap<String, String>,

//    How do we organize the metadata
}

impl TimePoint {
    pub fn new(timestamp: f64, value: f64) -> TimePoint {
        let map: BTreeMap<String, String> = BTreeMap::new();
        TimePoint { timestamp, value, metadata: map }
    }
}
