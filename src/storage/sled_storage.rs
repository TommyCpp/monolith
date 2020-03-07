use sled::{Db, Tree};
use crate::Result;
use crate::common::IdGenerator;
use std::path::Path;
use crate::storage::{Storage, Encoder};
use crate::common::label::Labels;
use crate::common::time_point::TimePoint;
use std::ops::Add;

const TIME_SERIES_PREFIX: &str = "TS";
const TIME_POINT_PREFIX: &str = "TP";
const LABEL_REVERSE_PREFIX: &str = "LR";


pub struct SledStorage {
    idx_generator: IdGenerator,
    storage: Db,
    serializer: SledEncoder,
}

impl SledStorage {
    pub fn new(p: &Path) -> Result<SledStorage> {
        Ok(SledStorage {
            idx_generator: IdGenerator::new(1),
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
        unimplemented!()
    }
}

struct SledEncoder {}

impl Encoder for SledEncoder {
    fn encode_time_point(time_stamp: u64, value: f64) -> Result<String> {
        Ok(format!("{},{}", time_stamp, value))
    }

    fn encode_time_series(mut time_series_meta: Labels) -> Result<String> {
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

        let result = SledEncoder::encode_time_series(labels).unwrap();

        assert_eq!("test1=test2,test2=test2,test4=test2,test5=test2", result)
    }
}
