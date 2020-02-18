use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::resume_unwind;
use std::time::{Duration, UNIX_EPOCH};


use std::iter::{Map, FromIterator};
use std::borrow::BorrowMut;
use crate::common::time_series::{TimeSeriesId, IdGenerator, TimeSeries};
use crate::common::label::{Labels, Label};
use crate::common::time_point::{Timestamp, Value};
use std::path::Path;


pub const CHUCK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_nanos() as Timestamp;

/// ChunkOps contains all options for chunk
struct ChunkOps<'c>{
    dir: &'c Path
}

///
/// Chunk store a set of time series fallen into certain time range;
/// Similar to the Block in Prometheus.
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
            end_time: start_time + CHUCK_SIZE,
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

    pub fn insert(&mut self, timestamp: Timestamp, value: Value, meta_data: Labels) {
        //if this time point does not belong to this chunk
        if !self.is_in_range(&timestamp) {
            return;
        }
        match self.get_series_to_insert(meta_data.vec()) {
            Some(time_series_id) => {
                self.time_series.get_mut(&time_series_id).unwrap().add(timestamp, value);
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
            Some(map) => {
                map.get(value)
            }
            None => { None }
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
