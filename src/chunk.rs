use std::collections::BTreeMap;
use std::panic::resume_unwind;
use std::time::{Duration, UNIX_EPOCH};

use crate::label::{Label, Labels};
use crate::time_series::{IdGenerator, TimeSeries, TimeSeriesId};

use super::time_point::*;

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
        //create index for every key_value
        for label in time_series.meta_date().vec() {
            let name = label.key().to_string();
            let value = label.value().to_string();
            if !self.time_series.contains_key(&name) {
                // if the name does not yet exist.
                self.time_series.insert(name.to_string(), BTreeMap::new());
            }
            if !self.time_series.get(&name).unwrap().contains_key(&value) {
                // if the value dost not yet exist.
                self.time_series.get_mut(&name).unwrap().insert(value, vec![id]);
            } else {
                // if the name and value are all exist, then we add id into it.
                //todo: insert in order to reduce the time complexity of intersection.
                let mut vec: &mut Vec<TimeSeriesId> = self.time_series.get_mut(&name).unwrap().get_mut(&value).unwrap();
                vec.push(id);
            }
        }
    }

    fn get_series_id_by_label(&self, label: &Label) -> Option<&Vec<TimeSeriesId>> {
        let key = label.key();
        let value = label.value();
        match self.time_series.get(key) {
            Some(map) => {
                map.get(value)
            }
            None => { None }
        }
    }

    pub fn insert(&mut self, timestamp: Timestamp, value: Value, meta_data: Labels) {}
}


#[cfg(test)]
mod test {
    use crate::chunk::{CHUCK_SIZE, Chunk};
    use crate::label::{Label, Labels};

    #[test]
    fn test_new_database() {
        let chunk = Chunk::new();
        assert_eq!(chunk.end_time - chunk.start_time, CHUCK_SIZE)
    }

    #[test]
    fn test_create_time_series() {
        let mut db = Chunk::new();
        let mut labels: Labels = Labels::new();
        labels.add(Label::new(String::from("test"), String::from("series")));
        db.create_series(labels, 12);
        assert_eq!(db.time_series.len(), 1)
    }

    #[test]
    fn test_get_series_id_by_labels() {
        //create timeseries
        let mut db = Chunk::new();
        let mut labels: Labels = Labels::new();
        labels.add(Label::new(String::from("test"), String::from("series")));
        db.create_series(labels.clone(), 12);

        //get series id by labels
        let query: Label = Label::new(String::from("test"), String::from("series"));
        {
            let res = db.get_series_id_by_label(&query).unwrap();
            assert_eq!(res.len(), 1);
            assert_eq!(*res.get(0).unwrap(), 12 as u64);
        }
        {
            db.create_series(labels.clone(), 11);
            let res = db.get_series_id_by_label(&query).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(*res.get(1).unwrap(), 11 as u64);
        }
    }
}
