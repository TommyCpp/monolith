use crate::chunk::Chunk;
use crate::option::{DbOpts, StorageType};
use crate::{Indexer, Result, ChunkOps, MonolithErr};

use crate::storage::{Storage, SledStorage};
use std::sync::{RwLock, Arc};
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::indexer::SledIndexer;
use crate::common::utils::get_current_timestamp;

pub struct MonolithDb<S: Storage, I: Indexer> {
    current_chuck: Chunk<S, I>,
    secondary_chunks: RwLock<Vec<Chunk<S, I>>>,
    options: DbOpts,
}

impl<S: Storage, I: Indexer> MonolithDb<S, I> {

    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        let _c = &self.current_chuck;
        let res = timepoints.into_iter().map(|tp| {
            _c.insert(labels.clone(), tp)
        }).filter(|res| res.is_err()).collect::<Vec<_>>();
        if res.len() > 0 {
            return Err(MonolithErr::InternalErr("Multiple time point cannot insert".to_string()));
        }
        Ok(())
    }

    pub fn write_time_point(&self, labels: Labels, timepoint: TimePoint) -> Result<()> {
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

impl MonolithDb<SledStorage, SledIndexer> {
    pub fn new(ops: DbOpts) -> Result<MonolithDb<SledStorage, SledIndexer>> {
        let chunk = {
            let indexer = SledIndexer::new(ops.base_dir().as_path().join("/indexer").as_path())?;
            let storage = SledStorage::new(ops.base_dir().as_path().join("/storage").as_path())?;
            let current_time = get_current_timestamp();
            let chunk_opt = ChunkOps {
                start_time: Some(current_time),
                end_time: Some(current_time + ops.chunk_size().as_millis() as Timestamp),
            };
            Chunk::<SledStorage, SledIndexer>::new(storage, indexer, &chunk_opt)
        };
        Ok(MonolithDb {
            current_chuck: chunk,
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
        })
    }
}
