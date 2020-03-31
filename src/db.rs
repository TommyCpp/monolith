use crate::chunk::{Chunk, ChunkOpts};
use crate::option::{DbOpts, StorageType};
use crate::{Result, MonolithErr};

use crate::storage::{Storage, SledStorage};
use std::sync::{RwLock, Arc};
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::indexer::{SledIndexer, Indexer};
use crate::common::utils::get_current_timestamp;

pub struct MonolithDb<S: Storage, I: Indexer> {
    current_chuck: Chunk<S, I>,
    secondary_chunks: RwLock<Vec<Chunk<S, I>>>,
    options: DbOpts,
}

/// NewDb trait specific different strategy to create different kind of MonolithDb with different underlying storage and indexer
pub trait NewDb
    where Self: Sized {
    type S: Storage;
    type I: Indexer;

    fn get_storage_and_indexer(ops: DbOpts) -> Result<(Self::S, Self::I)>;

    fn new(ops: DbOpts) -> Result<MonolithDb<Self::S, Self::I>> {
        let (storage, indexer) = Self::get_storage_and_indexer(ops.clone())?;
        let current_time = get_current_timestamp();
        let chunk_opt = ChunkOpts {
            start_time: Some(current_time),
            end_time: Some(current_time + ops.chunk_size().as_millis() as Timestamp),
        };
        let chunk = Chunk::<Self::S, Self::I>::new(storage, indexer, &chunk_opt);
        Ok(MonolithDb {
            current_chuck: chunk,
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
        })
    }
}

impl<S: Storage, I: Indexer> MonolithDb<S, I> {
    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        let _c = &self.current_chuck;
        let res = timepoints.into_iter().map(|tp| {
            if tp.timestamp == 0{
                return Ok(()) //skip null
            }
            _c.insert(labels.clone(), tp)
        }).filter(|res| res.is_err()).collect::<Vec<_>>();
        if res.len() > 0 {
            error!("num of error {}", res.len());
            return Err(MonolithErr::InternalErr("multiple time point cannot insert".to_string()));
        }
        Ok(())
    }

    pub fn write_time_point(&self, labels: Labels, timepoint: TimePoint) -> Result<()> {
        let _c = &self.current_chuck;
        _c.insert(labels.clone(), timepoint)?;
        Ok(())
    }

    pub fn query(
        &self,
        labels: Labels,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimeSeries>> {
        let _c = &self.current_chuck;
        _c.query(labels, start_time, end_time)
    }
}

impl NewDb for MonolithDb<SledStorage, SledIndexer> {
    type S = SledStorage;
    type I = SledIndexer;

    fn get_storage_and_indexer(ops: DbOpts) -> Result<(Self::S, Self::I)> {
        Ok((
            SledStorage::new(ops.base_dir().as_path().join("storage").as_path())?,
            SledIndexer::new(ops.base_dir().as_path().join("indexer").as_path())?,
        ))
    }
}
