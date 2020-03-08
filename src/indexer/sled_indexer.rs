use sled::Db;
use crate::indexer::Indexer;
use crate::common::label::Labels;
use crate::MonolithErr;
use crate::Result;
use crate::common::time_series::TimeSeriesId;
use std::ops::Add;

///
/// Sled based indexer, use to search timeseries id based on metadata.
pub struct SledIndexer {
    storage: Db
}

impl SeldIndexer {
    fn encode_labels(labels: Labels) -> Result<String> {
        let mut time_series_meta = labels.clone();
        time_series_meta.sort();
        let mut res = String::new();
        for label in time_series_meta.vec() {
            res = res.add(format!("{}={},", label.key(), label.value()).as_str());
        }
        res.pop(); //remove last ,
        Ok(res)
    }
}

impl Indexer for SledIndexer {
    fn get_series_id_by_labels(&self, labels: Labels) -> Result<TimeSeriesId> {
    }

    fn update_index(&self, labels: Labels, time_series_id: u64) -> Result<()> {
        let tree = &self.storage;
        //todo: search for every label and add time_series_id into it.

    }
}