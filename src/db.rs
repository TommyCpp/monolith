use crate::chunk::Chunk;
use crate::option::DbOpts;
use crate::{Indexer, Result};

use crate::storage::Storage;
use std::sync::RwLock;
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;

pub struct MonolithDb<S: Storage, I: Indexer> {
    current_chuck: Option<Chunk<S, I>>,
    secondary_chunks: RwLock<Vec<Chunk<S, I>>>,
    options: DbOpts,
}

impl<S: Storage, I: Indexer> MonolithDb<S, I> {
    pub fn new(ops: DbOpts) -> Result<MonolithDb<S, I>> {
        Ok(MonolithDb {
            current_chuck: None,
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
        })
    }

    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        unimplemented!()
    }

    pub fn write_time_point(&self, labels: Labels, timepoint: TimePoint) -> Result<()>{
        unimplemented!()
    }

    pub fn query(
        &self,
        labels: Labels,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimeSeries>> {
        unimplemented!()
    }
}
