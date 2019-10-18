use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use crate::time_series::{TimeSeriesId, IdGenerator, TimeSeries};

use super::time_point::*;
use crate::label::Labels;

pub const CHUCK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_nanos() as Timestamp;

pub struct Chunk {
    time_series: BTreeMap<String, BTreeMap<String, Vec<TimeSeriesId>>>,
    start_time: Timestamp,
    end_time: Timestamp,
}

impl Chunk {
    pub fn new() -> Chunk {
        let start_time = UNIX_EPOCH.elapsed().unwrap().as_nanos() as Timestamp;
        Chunk {
            time_series: BTreeMap::new(),
            start_time,
            end_time: start_time + CHUCK_SIZE,
        }
    }

    pub fn create_series(&mut self, labels: Labels, id: TimeSeriesId) -> () {
        //create series and assign id
        let time_series = TimeSeries::new(id, labels);
        for label in time_series.meta_date().vec() {
            let (name, value) = label.key_value();
            if !self.time_series.contains_key(&name) {
                self.time_series.insert(name, BTreeMap::new());
            } else {
                self.time_series.get_mut(&name).unwrap().insert(value, Vec::new());
            }
        }
    }
}


#[cfg(test)]
mod test {
    use crate::chunk::{Chunk, CHUCK_SIZE};
    use crate::label::{Labels, Label};

    #[test]
    fn test_new_database() {
        let chunk = Chunk::new();
        assert_eq!(chunk.end_time - chunk.start_time, CHUCK_SIZE)
    }

    #[test]
    fn test_insert_database() {
        let mut db = Chunk::new();
        let mut labels: Labels = Labels::new();
        labels.add(Label::new(String::from("test"), String::from("series")));
        db.create_series(labels, 12);
        assert_eq!(db.time_series.len(), 1)
    }
}
