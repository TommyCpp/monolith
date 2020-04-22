use crate::common::time_point::TimePoint;


use crate::storage::{Decoder, Encoder, Storage};
use crate::time_point::{Timestamp, Value};
use crate::MonolithErr::{NotFoundErr, OutOfRangeErr};
use crate::{Result, Builder, MonolithErr};
use sled::{Db, Tree};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::convert::TryInto;

const TIME_SERIES_PREFIX: &str = "TS";
const TIME_POINT_PREFIX: &str = "TP";

///
/// On-disk storage, only store data
/// do not in charge with id assign, index, label search. Those job should be given to chunk
#[derive(Clone)]
pub struct SledStorage {
    storage: Db,
}

impl SledStorage {
    pub fn new(p: &Path) -> Result<SledStorage> {
        Ok(SledStorage {
            storage: sled::Db::start_default(p)?,
        })
    }

    pub fn get_storage(self) -> Db {
        self.storage
    }

    fn parse_key_name<U: std::fmt::Display>(prefix: &str, key: U) -> String {
        format!("{}{}", prefix, key)
    }

    fn get_series_by_id(&self, time_series_id: u64) -> Result<Option<Vec<TimePoint>>> {
        let tree: &Tree = &self.storage;
        let key_name = SledStorage::parse_key_name::<u64>(TIME_SERIES_PREFIX, time_series_id);
        match tree.get(key_name)? {
            None => Err(NotFoundErr),
            Some(val) => {
                let val_vec = AsRef::<[u8]>::as_ref(&val).to_vec();
                let mut res: Vec<TimePoint> = Vec::new();
                for timepoint_bytes in val_vec.chunks(std::mem::size_of::<Timestamp>() + std::mem::size_of::<Value>()) {
                    res.push(SledProcessor::decode_time_point(timepoint_bytes)?);
                }

                return Ok(Some(res));
            }
        }
    }

    fn _read_time_series(
        series: Vec<TimePoint>,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimePoint>> {
        //series should already sorted
        let left = match series.binary_search(&TimePoint::new(start_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let right = match series.binary_search(&TimePoint::new(end_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };
        let res = &series[left..=right];
        Ok(Vec::from(res))
    }
}

impl Storage for SledStorage {
    //todo: check if timestamp is earlier than last timestamp in series. If so, how to deal with?
    fn write_time_point(&self, time_series_id: u64, timestamp: Timestamp, value: Value) -> Result<()> {
        let tree: &Tree = &self.storage;
        let key_name = SledStorage::parse_key_name::<u64>(TIME_SERIES_PREFIX, time_series_id);
        let mut value = SledProcessor::encode_time_point(timestamp, value)?;
        if let Some(current_val) = tree.get(key_name.clone())? {
            let mut current_val_bytes = current_val.to_vec();
            current_val_bytes.append(&mut value);
            tree.set(
                key_name.clone(),
                current_val_bytes
            );
        } else {
            tree.set(key_name.clone(), value)?;
        }
        Ok(())
    }

    fn read_time_series(
        &self,
        time_series_id: u64,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<TimePoint>> {
        let series = self.get_series_by_id(time_series_id)?.ok_or(NotFoundErr)?;
        if series.first().unwrap().timestamp > end_time
            || series.last().unwrap().timestamp < start_time
        {
            return Err(OutOfRangeErr(
                series.first().unwrap().timestamp,
                series.last().unwrap().timestamp,
            ));
        }
        SledStorage::_read_time_series(series, start_time, end_time)
    }

    fn read_from_existing(dir: PathBuf) -> Result<Self> {
        // Sled will create an empty db if there is nothing in dir.
        let config = sled::ConfigBuilder::default()
            .path(dir)
            .read_only(true);
        Ok(
            SledStorage {
                storage: sled::Db::start(config.build())?,
            }
        )
    }
}

//TODO: create a independent package for processor, create a KvProcessor for all key-value database
struct SledProcessor {}

impl Encoder for SledProcessor {
    fn encode_time_point(timestamp: u64, value: f64) -> Result<Vec<u8>> {
        let timestamp_bytes = timestamp.to_be_bytes();
        let value_bytes = value.to_be_bytes();
        let mut res = Vec::from(&timestamp_bytes[..]);
        res.extend_from_slice(&value_bytes[..]);
        Ok(res)
    }
}

impl Decoder for SledProcessor {
    fn decode_time_point(raw: &[u8]) -> Result<TimePoint> {
        if raw.len() != 16 {
            return Err(MonolithErr::InternalErr("Wrong number of bytes in input".to_string()));
        }
        let (timestamp_bytes, value_bytes) = raw.split_at(std::mem::size_of::<Timestamp>());
        Ok(
            TimePoint::new(Timestamp::from_be_bytes(timestamp_bytes.try_into().unwrap()), Value::from_be_bytes(value_bytes.try_into().unwrap()))
        )
    }
}

pub struct SledStorageBuilder {
    path: PathBuf
}

impl Builder<SledStorage> for SledStorageBuilder {
    fn build(&self, path: PathBuf) -> Result<SledStorage> {
        SledStorage::new(path.as_path().join("storage").as_path())
    }
}

impl SledStorageBuilder {
    pub fn new() -> Self {
        SledStorageBuilder {
            path: Default::default()
        }
    }
}

#[cfg(test)]
mod tests{
    use crate::Result;
    use tempfile::TempDir;
    use crate::storage::{SledStorage, Storage};
    use crate::common::test_utils::Ingester;

    #[test]
    fn test_storage() -> Result<()>{
        let temp_dir = TempDir::new()?;
        let storage = SledStorage::new(temp_dir.path())?;

        let ingester = Ingester::new(None, None, None, 10);
        for (idx, series) in ingester.data.iter().enumerate(){
            for tp in series.time_points() {
                storage.write_time_point(idx as u64, tp.timestamp, tp.value);
            }
        }

        for (idx, series) in ingester.data.iter().enumerate(){
            let res_series = storage.read_time_series(idx as u64, 0, 1000000)?;
            let expect_series = series.time_points();
            for idx in 0..expect_series.len(){
                assert_eq!(res_series[idx], expect_series[idx])
            }
        }


        Ok(())
    }
}