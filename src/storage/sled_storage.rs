use sled::{Db, Tree};
use crate::Result;
use crate::common::IdGenerator;
use std::path::Path;
use crate::storage::{Storage, Encoder};
use crate::common::label::Labels;
use crate::common::time_point::TimePoint;
use std::ops::{Add, Deref};
use crate::StorageType::SledBackendStorage;
use crate::MonolithErr::{NotFoundErr, OutOfRangeErr};

const TIME_SERIES_PREFIX: &str = "TS";
const TIME_POINT_PREFIX: &str = "TP";
const LABEL_REVERSE_PREFIX: &str = "LR";

///
/// On-disk storage, only store data
/// do not in charge with id assign, index, label search. Those job should be given to chunk
pub struct SledStorage {
    storage: Db,
    serializer: SledEncoder,
}

impl SledStorage {
    pub fn new(p: &Path) -> Result<SledStorage> {
        Ok(SledStorage {
            storage: sled::Db::start_default(p)?,
            serializer: SledEncoder {},
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
                    let timepoint: Vec<&str> = timepoint_str.split(",").collect();
                    let timestamp = timepoint.get(0).unwrap().deref().parse::<u64>()?;
                    let value = timepoint.get(1).unwrap().deref().parse::<f64>()?;
                    res.push(TimePoint::new(timestamp, value));
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
        let series = self.get_series_by_id(time_series_id)?.ok_or(NotFoundErr)?;
        if series.first().unwrap().timestamp < end_time || series.last().unwrap().timestamp > start_time {
            return Err(OutOfRangeErr(start_time, end_time));
        }
        //series should already sorted
        let left = match series.binary_search(&TimePoint::new(start_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx + 1
        };
        let right = match series.binary_search(&TimePoint::new(end_time, 0.0)) {
            Ok(idx) => idx,
            Err(idx) => idx - 1
        };
        let res = &series[left..=right];
        Ok(Vec::from(res))
    }
}

struct SledEncoder {}

impl Encoder for SledEncoder {
    fn encode_time_point(time_stamp: u64, value: f64) -> Result<String> {
        Ok(format!("{},{}", time_stamp, value))
    }

    fn encode_time_series_labels(mut time_series_meta: Labels) -> Result<String> {
        time_series_meta.sort();
        let mut res = String::new();
        for label in time_series_meta.vec() {
            res = res.add(format!("{}={},", label.key(), label.value()).as_str());
        }
        res.pop(); //remove last ,
        Ok(res)
    }
}


mod test {
    use crate::common::label::{Labels, Label};
    use crate::storage::sled_storage::SledEncoder;
    use crate::storage::Encoder;

    #[test]
    fn test_encoder_encode_time_series() {
        let mut labels = Labels::new();
        labels.add(Label::from("test1", "test2"));
        labels.add(Label::from("test5", "test2"));
        labels.add(Label::from("test4", "test2"));
        labels.add(Label::from("test2", "test2"));

        let result = SledEncoder::encode_time_series_labels(labels).unwrap();

        assert_eq!("test1=test2,test2=test2,test4=test2,test5=test2", result)
    }
}
