use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::resume_unwind;
use std::time::{Duration, UNIX_EPOCH};

use crate::label::{Label, Labels};
use crate::time_series::{IdGenerator, TimeSeries, TimeSeriesId};

use super::time_point::*;
use std::iter::{Map, FromIterator};
use std::borrow::BorrowMut;


pub const CHUCK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_nanos() as Timestamp;

pub struct Chunk {
    label_series: BTreeMap<String, BTreeMap<String, Vec<TimeSeriesId>>>,
    //store the map between the key-value label pair and time series id
    time_series: HashMap<TimeSeriesId, TimeSeries>,
    //store the actual time series
    start_time: Timestamp,
    end_time: Timestamp,
}

impl Chunk {
    pub fn new() -> Chunk {
        let start_time = UNIX_EPOCH.elapsed().unwrap().as_nanos() as Timestamp;
        Chunk {
            label_series: BTreeMap::new(),
            time_series: HashMap::new(),
            start_time,
            end_time: start_time + CHUCK_SIZE,
        }
    }

    pub fn create_series(&mut self, labels: Labels, id: TimeSeriesId) -> () {
        //create series and assign id
        let time_series = TimeSeries::new(id, labels);

        //create index for every key_value
        for label in time_series.meta_data().vec() {
            let name = label.key().to_string();
            let value = label.value().to_string();
            if !self.label_series.contains_key(&name) {
                // if the name does not yet exist.
                self.label_series.insert(name.to_string(), BTreeMap::new());
            }
            if !self.label_series.get(&name).unwrap().contains_key(&value) {
                // if the value dost not yet exist.
                self.label_series.get_mut(&name).unwrap().insert(value, vec![id]);
            } else {
                // if the name and value are all exist, then we add id into it.
                //todo: insert in order to reduce the time complexity of intersection.
                let mut vec: &mut Vec<TimeSeriesId> = self.label_series.get_mut(&name).unwrap().get_mut(&value).unwrap();
                vec.push(id);
            }
        }

        //insert time_series into storage
        self.time_series.insert(id, time_series);
    }

    fn get_series_id_by_label(&self, label: &Label) -> Option<&Vec<TimeSeriesId>> {
        let key = label.key();
        let value = label.value();
        match self.label_series.get(key) {
            Some(map) => {
                map.get(value)
            }
            None => { None }
        }
    }

    /**
    *  get the target series to insert new time point, if no such time series exists, then return None
    */
    fn get_series_to_insert(&self, labels: Vec<&Label>) -> Option<TimeSeriesId> {
        let labels_len = labels.len();
        let mut candidates: Vec<HashSet<TimeSeriesId>> = Vec::new();
        for label in labels {
            match self.get_series_id_by_label(label) {
                Some(v) => {
                    candidates.push(HashSet::from_iter(v.iter().cloned()));
                }
                None => {}
            }
        }
        //intersect among the candidates
        if candidates.len() != 0 {
            let res: HashSet<TimeSeriesId> = candidates[0].clone();
            for i in 1..candidates.len() {
                res.intersection(&candidates[i]);
            }
            if res.len() > 0 {
                for v in res {
                    if self.time_series.get(&v).unwrap().meta_data().len() == labels_len {
                        return Some(v);
                    }
                }
            }
            return None; // error handling
        }
        return None;
    }

    pub fn insert(&mut self, timestamp: Timestamp, value: Value, meta_data: Labels) {}
}


#[cfg(test)]
mod test {
    use crate::chunk::{CHUCK_SIZE, Chunk};
    use crate::label::{Label, Labels};
    use std::collections::BTreeMap;
    use crate::time_series::TimeSeriesId;


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
        assert_eq!(db.label_series.len(), 1)
    }

    #[test]
    fn test_get_series_id_by_label() {
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

    #[test]
    fn test_get_series_id_by_labels() {
        let mut db = Chunk::new();
        let mut i = 0;
        let mut labels_ts_1 = Labels::new();
        labels_ts_1.add(Label::new(String::from("test1"), String::from("value1")));
        labels_ts_1.add(Label::new(String::from("test2"), String::from("value2")));
        labels_ts_1.add(Label::new(String::from("test3"), String::from("value2")));
        labels_ts_1.add(Label::new(String::from("test4"), String::from("value2")));

        let mut labels_ts_2 = Labels::new();
        labels_ts_2.add(Label::new(String::from("test1"), String::from("value1")));
        labels_ts_2.add(Label::new(String::from("test2"), String::from("value2")));
        labels_ts_2.add(Label::new(String::from("test3"), String::from("value2")));

        db.create_series(labels_ts_1.clone(), 11);
        db.create_series(labels_ts_2.clone(), 12);

        let label1 = Label::new(String::from("test1"), String::from("value1"));
        let label2 = Label::new(String::from("test2"), String::from("value2"));
        let label3 = Label::new(String::from("test3"), String::from("value2"));


        let mut target = vec![&label1, &label2, &label3];
        match db.get_series_to_insert(target) {
            Some(res) => {
                assert_eq!(res, 12)
            }
            None => {
                assert_eq!(true, false) //fail the test
            }
        }
    }
}
