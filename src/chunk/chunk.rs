use std::collections::{BTreeMap, HashMap, HashSet};

use std::time::{Duration, UNIX_EPOCH};

use crate::common::label::{Label, Labels};
use crate::common::time_point::{Timestamp, Value};
use crate::common::time_series::{TimeSeries, TimeSeriesId};
use crate::common::IdGenerator;

use std::iter::FromIterator;
use std::path::PathBuf;

pub const DEFAULT_CHUNK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_nanos() as Timestamp;

/// ChunkOps contains all options for chunk
struct ChunkOps {
    dir: PathBuf,
    chunk_size: u64, // as sec
}

///
/// Chunk store a set of time series fallen into certain time range;
///
pub struct Chunk {
    //store the map between the key-value label pair and time series id
    time_series: HashMap<TimeSeriesId, TimeSeries>,
    //store the actual time series
    label_series: BTreeMap<String, BTreeMap<String, Vec<TimeSeriesId>>>,
    start_time: Timestamp,
    end_time: Timestamp,
    closed: bool,
    id_generator: IdGenerator,
}

impl Chunk {
    pub fn new() -> Self {
        let start_time = UNIX_EPOCH.elapsed().unwrap().as_nanos() as Timestamp;
        Chunk {
            label_series: BTreeMap::new(),
            time_series: HashMap::new(),
            start_time,
            end_time: start_time + DEFAULT_CHUNK_SIZE,
            closed: false,
            id_generator: IdGenerator::new(0),
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
                self.label_series
                    .get_mut(&name)
                    .unwrap()
                    .insert(value, vec![id]);
            } else {
                // if the name and value are all exist, then we add id into it.
                //
                // todo: insert in order to reduce the time complexity of intersection.
                let vec: &mut Vec<TimeSeriesId> = self
                    .label_series
                    .get_mut(&name)
                    .unwrap()
                    .get_mut(&value)
                    .unwrap();
                vec.push(id);
            }
        }

        //insert time_series into storage
        self.time_series.insert(id, time_series);
    }

    pub fn insert(&mut self, timestamp: Timestamp, value: Value, meta_data: Labels) {
        //if this time point does not belong to this chunk
        if !self.is_in_range(&timestamp) {
            return;
        }
        match self.get_series_to_insert(meta_data.vec()) {
            Some(time_series_id) => {
                self.time_series
                    .get_mut(&time_series_id)
                    .unwrap()
                    .add(timestamp, value);
            }
            None => {
                self.create_series(meta_data, self.id_generator.next());
            }
        }
    }

    fn get_series_id_by_label(&self, label: &Label) -> Option<&Vec<TimeSeriesId>> {
        let key = label.key();
        let value = label.value();
        match self.label_series.get(key) {
            Some(map) => map.get(value),
            None => None,
        }
    }

    fn is_in_range(&self, timestamp: &Timestamp) -> bool {
        return self.start_time < *timestamp && self.end_time > *timestamp;
    }

    /**
     *  get the target series to insert new time point, if no such time series exists, then return None
     */
    fn get_series_to_insert(&self, labels: &Vec<Label>) -> Option<TimeSeriesId> {
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
}

fn get_chunk_size(sec: u64) -> Timestamp {
    Duration::from_secs(sec * 60 * 60).as_nanos() as Timestamp
}

pub struct ChunkBuilder {}

#[cfg(test)]
mod test {
    use crate::chunk::chunk::DEFAULT_CHUNK_SIZE;
    use crate::common::label::{Label, Labels};
    use crate::common::time_point::Timestamp;

    use crate::Chunk;

    #[test]
    fn test_timestamp_in_range() {
        let mut db = Chunk::new();
        db.start_time = 10000 as Timestamp;
        db.end_time = 15000 as Timestamp;
        let ts1 = 12000 as Timestamp;
        let ts2 = 1000 as Timestamp;
        assert_eq!(db.is_in_range(&ts1), true);
        assert_eq!(db.is_in_range(&ts2), false);
    }

    #[test]
    fn test_insert() {
        let mut db = Chunk::new();
        db.start_time = 10000 as Timestamp;
        db.end_time = 15000 as Timestamp;
        let mut meta_data = Labels::new();
        let label_x = Label::from("x", "y");
        meta_data.add(label_x);
        db.insert(11000 as Timestamp, 10 as f64, meta_data);
        let res = db.get_series_id_by_label(&Label::from("x", "y")).unwrap();
        assert_eq!(1, res.len());
        assert_eq!(res.get(0), Some(&0));
    }

    #[test]
    fn test_get_series_id_by_labels() {
        let mut db = Chunk::new();
        let _i = 0;
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

        let target = vec![label1, label2, label3];
        match db.get_series_to_insert(&target) {
            Some(res) => assert_eq!(res, 12),
            None => {
                assert_eq!(true, false) //fail the test
            }
        }
    }

    #[test]
    fn test_new_database() {
        let chunk = Chunk::new();
        assert_eq!(chunk.end_time - chunk.start_time, DEFAULT_CHUNK_SIZE)
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
}
