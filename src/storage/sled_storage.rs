use sled::Db;
use crate::Result;
use crate::common::IdGenerator;
use std::path::Path;

pub struct SledStorage {
    index: IdGenerator,
    storage: Db,
}

impl SledStorage {
    fn new(p: &Path) -> Result<SledStorage> {
        Ok(SledStorage {
            index: IdGenerator::new(1),
            storage: sled::Db::start_default(p)?
        })
    }
}