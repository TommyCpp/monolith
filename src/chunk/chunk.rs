

use std::time::{Duration, UNIX_EPOCH};

use crate::{Result, MonolithErr};
use crate::common::label::{Labels};
use crate::common::time_point::{Timestamp, TimePoint};
use crate::common::time_series::{TimeSeries};
use crate::common::IdGenerator;


use std::path::PathBuf;
use crate::storage::Storage;
use crate::Indexer;

pub const DEFAULT_CHUNK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_nanos() as Timestamp;

/// ChunkOps contains all options for chunk
struct ChunkOps {
    dir: PathBuf,
    chunk_size: u64, // as sec
}

///
/// Chunk store a set of time series fallen into certain time range;
///
pub struct Chunk<S: Storage, I: Indexer> {
    storage: S,
    indexer: I,
    start_time: Timestamp,
    end_time: Timestamp,
    closed: bool,
    id_generator: IdGenerator,
}


//todo: add concurrent control
impl<S: Storage, I: Indexer> Chunk<S, I> {
    pub fn new(storage: S, indexer: I) -> Self {
        let start_time = UNIX_EPOCH.elapsed().unwrap().as_nanos() as Timestamp;
        Chunk {
            storage,
            indexer,
            start_time,
            end_time: start_time + DEFAULT_CHUNK_SIZE,
            closed: false,
            id_generator: IdGenerator::new(0),
        }
    }

    pub fn insert(&self, labels: Labels, timepoint: TimePoint) -> Result<()> {
        if !self.is_in_range(&timepoint.timestamp) {
            return Err(MonolithErr::OutOfRangeErr(self.start_time, self.end_time));
        }
        let id = self.indexer.get_series_id_by_labels(labels.clone())?;
        if id.is_none() {
            //insert new
            let new_id = self.id_generator.next();
            self.indexer.create_index(labels.clone(), new_id)?;
            self.storage.write_time_point(new_id, timepoint.timestamp, timepoint.value)?;
        } else {
            //only one, skip the choose process
            self.storage.write_time_point(id.unwrap(), timepoint.timestamp, timepoint.value);
        }

        Ok(())
    }

    pub fn query(&self, labels: Labels, start_time: Timestamp, end_time: Timestamp) -> Result<Vec<TimeSeries>> {
        let candidates = self.indexer.get_series_with_label_matching(labels)?;
        let mut res = Vec::new();
        for (id, metadata) in candidates {
            let data = self.storage.read_time_series(id, start_time, end_time)?;
            if data.len() == 0 {
                continue; //skip empty series
            }
            res.push(TimeSeries::from(id, metadata, data))
        }
        Ok(res)
    }

    fn is_in_range(&self, timestamp: &Timestamp) -> bool {
        return self.start_time < *timestamp && self.end_time > *timestamp;
    }
}

fn get_chunk_size(sec: u64) -> Timestamp {
    Duration::from_secs(sec * 60 * 60).as_nanos() as Timestamp
}

pub struct ChunkBuilder {}

#[cfg(test)]
mod test {
    // use crate::chunk::chunk::DEFAULT_CHUNK_SIZE;
    // use crate::common::label::{Label, Labels};
    // use crate::common::time_point::Timestamp;
    //
    // use crate::Chunk;
    //
    // #[test]
    // fn test_timestamp_in_range() {
    //     let mut db = Chunk::new();
    //     db.start_time = 10000 as Timestamp;
    //     db.end_time = 15000 as Timestamp;
    //     let ts1 = 12000 as Timestamp;
    //     let ts2 = 1000 as Timestamp;
    //     assert_eq!(db.is_in_range(&ts1), true);
    //     assert_eq!(db.is_in_range(&ts2), false);
    // }
    //
    // #[test]
    // fn test_insert() {
    //     let mut db = Chunk::new();
    //     db.start_time = 10000 as Timestamp;
    //     db.end_time = 15000 as Timestamp;
    //     let mut meta_data = Labels::new();
    //     let label_x = Label::from("x", "y");
    //     meta_data.add(label_x);
    //     db.insert(11000 as Timestamp, 10 as f64, meta_data);
    //     let res = db.get_series_id_by_label(&Label::from("x", "y")).unwrap();
    //     assert_eq!(1, res.len());
    //     assert_eq!(res.get(0), Some(&0));
    // }
    //
    // #[test]
    // fn test_get_series_id_by_labels() {
    //     let mut db = Chunk::new();
    //     let _i = 0;
    //     let mut labels_ts_1 = Labels::new();
    //     labels_ts_1.add(Label::new(String::from("test1"), String::from("value1")));
    //     labels_ts_1.add(Label::new(String::from("test2"), String::from("value2")));
    //     labels_ts_1.add(Label::new(String::from("test3"), String::from("value2")));
    //     labels_ts_1.add(Label::new(String::from("test4"), String::from("value2")));
    //
    //     let mut labels_ts_2 = Labels::new();
    //     labels_ts_2.add(Label::new(String::from("test1"), String::from("value1")));
    //     labels_ts_2.add(Label::new(String::from("test2"), String::from("value2")));
    //     labels_ts_2.add(Label::new(String::from("test3"), String::from("value2")));
    //
    //     db.create_series(labels_ts_1.clone(), 11);
    //     db.create_series(labels_ts_2.clone(), 12);
    //
    //     let label1 = Label::new(String::from("test1"), String::from("value1"));
    //     let label2 = Label::new(String::from("test2"), String::from("value2"));
    //     let label3 = Label::new(String::from("test3"), String::from("value2"));
    //
    //     let target = vec![label1, label2, label3];
    //     match db.get_series_to_insert(&target) {
    //         Some(res) => assert_eq!(res, 12),
    //         None => {
    //             assert_eq!(true, false) //fail the test
    //         }
    //     }
    // }
    //
    // #[test]
    // fn test_new_database() {
    //     let chunk = Chunk::new();
    //     assert_eq!(chunk.end_time - chunk.start_time, DEFAULT_CHUNK_SIZE)
    // }
    //
    // #[test]
    // fn test_create_time_series() {
    //     let mut db = Chunk::new();
    //     let mut labels: Labels = Labels::new();
    //     labels.add(Label::new(String::from("test"), String::from("series")));
    //     db.create_series(labels, 12);
    //     assert_eq!(db.label_series.len(), 1)
    // }
    //
    // #[test]
    // fn test_get_series_id_by_label() {
    //     //create timeseries
    //     let mut db = Chunk::new();
    //     let mut labels: Labels = Labels::new();
    //     labels.add(Label::new(String::from("test"), String::from("series")));
    //     db.create_series(labels.clone(), 12);
    //
    //     //get series id by labels
    //     let query: Label = Label::new(String::from("test"), String::from("series"));
    //     {
    //         let res = db.get_series_id_by_label(&query).unwrap();
    //         assert_eq!(res.len(), 1);
    //         assert_eq!(*res.get(0).unwrap(), 12 as u64);
    //     }
    //     {
    //         db.create_series(labels.clone(), 11);
    //         let res = db.get_series_id_by_label(&query).unwrap();
    //         assert_eq!(res.len(), 2);
    //         assert_eq!(*res.get(1).unwrap(), 11 as u64);
    //     }
    // }
}
