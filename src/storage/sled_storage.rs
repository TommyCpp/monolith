use crate::common::time_point::TimePoint;


use crate::storage::{Decoder, Encoder, Storage};
use crate::MonolithErr::{NotFoundErr, OutOfRangeErr};
use crate::{Result, Builder, MonolithErr, Timestamp, Value, HasTypeName};
use sled::{Db, Tree};

use std::path::{Path, PathBuf};
use std::convert::TryInto;
use crate::chunk::ChunkOpts;
use crate::option::DbOpts;

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
                    res.push(KvStorageProcessor::decode_time_point(timepoint_bytes)?);
                }

                return Ok(Some(res));
            }
        }
    }
}

impl Storage for SledStorage {
    //todo: check if timestamp is earlier than last timestamp in series. If so, how to deal with?
    fn write_time_point(&self, time_series_id: u64, timestamp: Timestamp, value: Value) -> Result<()> {
        let tree: &Tree = &self.storage;
        let key_name = SledStorage::parse_key_name::<u64>(TIME_SERIES_PREFIX, time_series_id);
        let mut value = KvStorageProcessor::encode_time_point(timestamp, value)?;
        if let Some(current_val) = tree.get(key_name.clone())? {
            let mut current_val_bytes = current_val.to_vec();
            current_val_bytes.append(&mut value);
            tree.set(
                key_name.clone(),
                current_val_bytes,
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
        SledStorage::trim_time_series(series, start_time, end_time)
    }

}

impl HasTypeName for SledStorage {
    fn get_type_name() -> &'static str {
        return "SledStorage";
    }
}

//TODO: create a independent package for processor, create a KvProcessor for all key-value database
pub(crate) struct KvStorageProcessor {}

impl Encoder for KvStorageProcessor {
    fn encode_time_point(timestamp: Timestamp, value: Value) -> Result<Vec<u8>> {
        let timestamp_bytes = timestamp.to_be_bytes();
        let value_bytes = value.to_be_bytes();
        let mut res = Vec::from(&timestamp_bytes[..]);
        res.extend_from_slice(&value_bytes[..]);
        Ok(res)
    }
}

impl Decoder for KvStorageProcessor {
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

pub struct SledStorageBuilder {}

impl Builder<SledStorage> for SledStorageBuilder {
    fn build(&self, path: String, _: Option<&ChunkOpts>, _: Option<&DbOpts>) -> Result<SledStorage> {
        SledStorage::new(PathBuf::from(path).as_path().join("storage").as_path())
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_from_chunk(&self, dir: &Path, _: Option<&ChunkOpts>) -> Result<Option<SledStorage>> {
        // Sled will create an empty db if there is nothing in dir.
        let config = sled::ConfigBuilder::default()
            .path(dir)
            .read_only(true);
        Ok(
            Some(SledStorage {
                storage: sled::Db::start(config.build())?,
            })
        )
    }

    fn write_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }
}

impl SledStorageBuilder {
    pub fn new() -> Self {
        SledStorageBuilder {}
    }
}