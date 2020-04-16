use std::path::PathBuf;
use crate::storage::Storage;
use crate::indexer::Indexer;

use crate::{Result, Builder};
use crate::label::Labels;
use crate::time_point::TimePoint;

struct StubStorage {}

impl Storage for StubStorage {
    fn write_time_point(&self, _time_series_id: u64, _timestamp: u64, _value: f64) -> Result<()> {
        unimplemented!()
    }

    fn read_time_series(&self, _time_series_id: u64, _start_time: u64, _end_time: u64) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }

    fn read_from_existing(dir: PathBuf) -> Result<Self> {
        unimplemented!()
    }
}

impl Builder<StubStorage> for StubStorage {
    fn build(&self, path: PathBuf) -> Result<StubStorage> {
        Ok(StubStorage {})
    }
}

struct StubIndexer {}

impl Indexer for StubIndexer {
    fn get_series_with_label_matching(&self, _labels: Labels) -> Result<Vec<(u64, Labels)>> {
        unimplemented!()
    }

    fn get_series_id_with_label_matching(&self, _labels: Labels) -> Result<Vec<u64>> {
        unimplemented!()
    }

    fn get_series_id_by_labels(&self, _labels: Labels) -> Result<Option<u64>> {
        unimplemented!()
    }

    fn create_index(&self, _labels: Labels, _time_series_id: u64) -> Result<()> {
        unimplemented!()
    }

    fn read_from_existing(dir: PathBuf) -> Result<Self> {
        unimplemented!()
    }
}

impl Builder<StubIndexer> for StubIndexer {
    fn build(&self, path: PathBuf) -> Result<StubIndexer> {
        Ok(StubIndexer{})
    }
}