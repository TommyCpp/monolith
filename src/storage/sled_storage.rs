use sled::{Db, Tree};
use crate::Result;
use crate::common::IdGenerator;
use std::path::Path;
use crate::storage::{Storage, Encoder};
use crate::common::label::Labels;
use crate::common::time_point::TimePoint;

const TIME_SERIES_PREFIX: &str = "TS";
const TIME_POINT_PREFIX: &str = "TP";


pub struct SledStorage {
    index: IdGenerator,
    storage: Db,
    serializer: SledEncoder,
}

impl SledStorage {
    pub fn new(p: &Path) -> Result<SledStorage> {
        Ok(SledStorage {
            index: IdGenerator::new(1),
            storage: sled::Db::start_default(p)?,
            serializer: SledEncoder {},
        })
    }

    pub fn get_storage(self) -> Db {
        self.storage
    }
}

impl Storage for SledStorage {
    fn write_time_point(&self, time_series_id: u64, timestamp: u64, value: f64) -> Result<()> {
        let tree: &Tree = &self.storage;
        let key_name = format!("{}-{}", TIME_SERIES_PREFIX, time_series_id);
        let value = SledEncoder::encode_time_point(timestamp, value)?.into_bytes();
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

    fn read_time_series(&self, time_series_id: u64, start_time: u64, end_time: u64) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }
}

struct SledEncoder {}

impl Encoder for SledEncoder {
    fn encode_time_point(time_stamp: u64, value: f64) -> Result<String> {
        Ok(format!("{},{}", time_stamp, value))
    }

    fn encode_time_series(time_series_meta: Labels) -> Result<String> {
        unimplemented!()
    }
}
