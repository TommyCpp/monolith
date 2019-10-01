use std::collections::BTreeMap;

pub type Timestamp = f64;
pub type Value = f64;

pub struct TimePoint {
    timestamp: Timestamp,
    value: Value,

//    How do we organize the metadata
}

impl TimePoint {
    pub fn new(timestamp: f64, value: f64) -> TimePoint {
        TimePoint { timestamp, value }
    }
}

pub struct Chuck {
    timestamps: Vec<TimePoint>
}

impl Chuck {
    pub fn new() -> Chuck {
        let timestamps = Vec::new();

        Chuck { timestamps }
    }
}


#[cfg(test)]
mod test {
    use crate::common::*;

    #[test]
    fn create_chuck() {
        let chuck = Chuck::new();
        assert_eq!(chuck.timestamps.len(), 0)
    }

    #[test]
    fn create_timepoint() {
        let timepoint = TimePoint::new(120.0, 12.0);
        assert_eq!(timepoint.timestamp, 120.0);
        assert_eq!(timepoint.value, 12.0);
    }
}