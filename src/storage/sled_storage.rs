use crate::common::label::Labels;
use crate::common::time_point::TimePoint;
use crate::common::IdGenerator;
use crate::storage::{Decoder, Encoder, Storage};
use crate::MonolithErr::{NotFoundErr, OutOfRangeErr};
use crate::{MonolithErr, Result};
use sled::{Db, Tree};
use std::ops::{Add, Deref};
use std::path::Path;

const TIME_SERIES_PREFIX: &str = "TS";
const TIME_POINT_PREFIX: &str = "TP";

///
/// On-disk storage, only store data
/// do not in charge with id assign, index, label search. Those job should be given to chunk
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
        format!("{},{}", prefix, key)
    }

    fn get_series_by_id(&self, time_series_id: u64) -> Result<Option<Vec<TimePoint>>> {
        let tree: &Tree = &self.storage;
        let key_name = SledStorage::parse_key_name::<u64>(TIME_SERIES_PREFIX, time_series_id);
        match tree.get(key_name)? {
            None => Err(NotFoundErr),
            Some(val) => {
                let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
                let timepoint_strs: Vec<&str> = val_str.split("/").collect();
                let mut res: Vec<TimePoint> = Vec::new();
                for timepoint_str in timepoint_strs {
                    res.push(SledProcessor::decode_time_point(String::from(
                        timepoint_str,
                    ))?);
                }

                return Ok(Some(res));
            }
        }
    }
}

impl Storage for SledStorage {
    fn write_time_point(&self, time_series_id: u64, timestamp: u64, value: f64) -> Result<()> {
        let tree: &Tree = &self.storage;
        let key_name = SledStorage::parse_key_name::<u64>(TIME_SERIES_PREFIX, time_series_id);
        let value = SledProcessor::encode_time_point(timestamp, value)?.into_bytes();
        if let Some(current_val) = tree.get(key_name.clone())? {
            let current_val_u8 = String::from_utf8(current_val.to_vec())?;
            tree.set(
                key_name.clone(),
                format!("{}/{}", current_val_u8, String::from_utf8(value)?).into_bytes(),
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
        if series.first().unwrap().timestamp < end_time
            || series.last().unwrap().timestamp > start_time
        {
            return Err(OutOfRangeErr(start_time, end_time));
        }
        //series should already sorted
        let left = match series.binary_search(&TimePoint::new(start_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx + 1,
        };
        let right = match series.binary_search(&TimePoint::new(end_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };
        let res = &series[left..=right];
        Ok(Vec::from(res))
    }
}

struct SledProcessor {}

impl Encoder for SledProcessor {
    fn encode_time_point(time_stamp: u64, value: f64) -> Result<String> {
        Ok(format!("{},{}", time_stamp, value))
    }
}

impl Decoder for SledProcessor {
    fn decode_time_point(raw: String) -> Result<TimePoint> {
        let timepoint: Vec<&str> = raw.split(",").collect();
        let timestamp = timepoint.get(0).unwrap().deref().parse::<u64>()?;
        let value = timepoint.get(1).unwrap().deref().parse::<f64>()?;
        Ok(TimePoint::new(timestamp, value))
    }
}
