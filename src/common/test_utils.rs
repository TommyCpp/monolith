use crate::indexer::Indexer;
use crate::storage::Storage;
use std::path::Path;

use crate::common::label::Label;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeries;
use crate::label::Labels;
use crate::time_series::TimeSeriesId;
use crate::{Builder, HasTypeName, Result, Timestamp, Value};

use crate::backend::tikv::TiKvRawBackend;
use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

///Stub Storage for testing
pub struct StubStorage {}

impl HasTypeName for StubStorage {
    fn get_type_name() -> &'static str {
        "StubStorage"
    }
}

impl Storage for StubStorage {
    fn write_time_point(&self, _time_series_id: u64, _timestamp: u64, _value: f64) -> Result<()> {
        unimplemented!()
    }

    fn read_time_series(
        &self,
        _time_series_id: u64,
        _start_time: u64,
        _end_time: u64,
    ) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }
}

impl Builder<StubStorage> for StubStorage {
    fn build(
        &self,
        _path: String,
        _: Option<&ChunkOpts>,
        _: Option<&DbOpts>,
    ) -> Result<StubStorage> {
        Ok(StubStorage {})
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }

    fn read_from_chunk(&self, _dir: &Path, _: Option<&ChunkOpts>) -> Result<Option<StubStorage>> {
        unimplemented!()
    }

    fn write_config(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }
}

///Stub indexer for testing
pub struct StubIndexer {}

impl HasTypeName for StubIndexer {
    fn get_type_name() -> &'static str {
        "StubIndexer"
    }
}

impl Indexer for StubIndexer {
    fn get_series_metadata_contains_labels(&self, _labels: Labels) -> Result<Vec<(u64, Labels)>> {
        unimplemented!()
    }

    fn get_series_id_contains_labels(&self, _labels: Labels) -> Result<Vec<u64>> {
        unimplemented!()
    }

    fn get_series_id_by_labels(&self, _labels: Labels) -> Result<Option<u64>> {
        unimplemented!()
    }

    fn create_index(&self, _labels: Labels, _time_series_id: u64) -> Result<()> {
        unimplemented!()
    }
}

impl Builder<StubIndexer> for StubIndexer {
    fn build(
        &self,
        _path: String,
        _: Option<&ChunkOpts>,
        _: Option<&DbOpts>,
    ) -> Result<StubIndexer> {
        Ok(StubIndexer {})
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }

    fn read_from_chunk(&self, _dir: &Path, _: Option<&ChunkOpts>) -> Result<Option<StubIndexer>> {
        unimplemented!()
    }

    fn write_config(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        unimplemented!()
    }
}

//todo: add internal concurrent test function, use a closure as param for real test logic.
/// Ingester generate and ingest time series data with volume of user's choice.
pub struct Ingester {
    pub data: Vec<TimeSeries>,
}

impl Ingester {
    /// Create Ingester
    ///
    /// If the num_series is provided, then data will contains num_series time series.
    /// If not, then will choose a volume randomly from 10 to 1000
    ///
    ///If the num_time_point is provided, then each time series generated will have the num_time_point time points.
    ///If not, then each time series will have random num of time points from 10 to 100
    ///
    /// Same goes for the num_labels, if no value provided, then will randomly pick between 10 to 30 as num_labels
    ///
    /// All time point will have timestamps from start_time increasing by 100.
    pub fn new(
        num_series: Option<usize>,
        num_time_point: Option<usize>,
        num_labels: Option<usize>,
        start_time: Timestamp,
    ) -> Ingester {
        Ingester {
            data: (0..num_series
                .unwrap_or(rand::thread_rng().gen_range(10 as usize, 1000 as usize)))
                .map(|_n| Ingester::_generate_data(num_time_point, num_labels, start_time))
                .collect::<Vec<TimeSeries>>(),
        }
    }

    pub fn from_data(
        ids: Vec<TimeSeriesId>,
        metadata: Vec<Vec<(&str, &str)>>,
        data: Vec<Vec<(Timestamp, Value)>>,
    ) -> Ingester {
        assert_eq!(ids.len(), metadata.len());
        assert_eq!(data.len(), metadata.len());

        let mut res = Vec::new();
        for i in 0..ids.len() {
            let mut meta = Vec::new();
            for d in metadata.get(i).unwrap() {
                meta.push(Label::from_key_value(d.clone().0, d.clone().1));
            }
            let mut time_points = Vec::new();
            for t in data.get(i).unwrap() {
                time_points.push(TimePoint::new(t.clone().0, t.clone().1));
            }
            let time_series =
                TimeSeries::from_data(*(ids.get(i).unwrap()), Labels::from_vec(meta), time_points);
            res.push(time_series);
        }

        Ingester { data: res }
    }

    fn _generate_data(
        num_time_point: Option<usize>,
        num_labels: Option<usize>,
        start_time: Timestamp,
    ) -> TimeSeries {
        let tp_size = num_time_point.unwrap_or(rand::thread_rng().gen_range(10, 100));
        let label_size = num_labels.unwrap_or(rand::thread_rng().gen_range(10, 30));

        //generate time point
        let mut tps = vec![TimePoint {
            timestamp: start_time,
            value: rand::thread_rng().gen_range(15.0, 199.0),
        }];
        for _ in 1..tp_size {
            let last = tps.last().unwrap().timestamp;
            tps.push(TimePoint {
                timestamp: last + 100 as Timestamp,
                value: rand::thread_rng().gen_range(15.0, 199.0),
            });
        }

        //generate labels
        let mut labels = vec![];
        for _ in 0..label_size {
            let key = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect();
            let val = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect();
            labels.push(Label::new(key, val));
        }

        TimeSeries::from_data(0, Labels::from_vec(labels), tps)
    }
}

pub struct DummyTiKvBackend {
    tree: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl Clone for DummyTiKvBackend {
    fn clone(&self) -> Self {
        DummyTiKvBackend {
            tree: self.tree.clone(),
        }
    }
}

impl DummyTiKvBackend {
    pub fn new() -> DummyTiKvBackend {
        DummyTiKvBackend {
            tree: Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new())),
        }
    }
}

impl TiKvRawBackend for DummyTiKvBackend {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut map = self.tree.lock().unwrap();
        map.insert(key, value);
        Ok(())
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let map = self.tree.lock().unwrap();
        let get_res = map.get(&key);
        if get_res.is_some() {
            Ok(Some(get_res.unwrap().clone()))
        } else {
            Ok(None)
        }
    }
}
