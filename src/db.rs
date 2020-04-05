use crate::chunk::{Chunk, ChunkOpts};
use crate::option::DbOpts;
use crate::{Result, MonolithErr, TIME_UNIT, Builder};

use crate::storage::{Storage, SledStorage};
use std::sync::{RwLock, Arc};
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::indexer::{SledIndexer, Indexer};
use crate::common::utils::get_current_timestamp;
use std::cell::{Cell, RefCell};
use std::thread;
use std::sync::mpsc::{Receiver, channel};

/// MonolithDb is thread-safe
pub struct MonolithDb<S: Storage, I: Indexer> {
    current_chuck: RwLock<Arc<Chunk<S, I>>>,
    secondary_chunks: RwLock<Vec<Arc<Chunk<S, I>>>>,
    //todo: chunk swap
    options: DbOpts,
    storage_builder: Box<dyn Builder<S> + Sync + Send>,
    indexer_builder: Box<dyn Builder<I> + Sync + Send>
}

impl<S: Storage, I: Indexer> MonolithDb<S, I> {
    pub fn new(ops: DbOpts, mut storage_builder: Box<dyn Builder<S> + Sync + Send>, mut indexer_builder: Box<dyn Builder<I> + Sync + Send>) -> Result<Self>{
        let (storage, indexer) = (storage_builder.build()?, indexer_builder.build()?);
        let current_time = get_current_timestamp();
        let chunk_opt = ChunkOpts {
            start_time: Some(current_time),
            end_time: Some(current_time + ops.chunk_size().as_millis() as Timestamp),
        };
        let chunk = Chunk::<S, I>::new(storage, indexer, &chunk_opt);
        let (swap_tx, swap_rx) = channel::<Timestamp>();
        let db = MonolithDb {
            current_chuck: RwLock::new(Arc::new(chunk)),
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
            storage_builder,
            indexer_builder
        };
        thread::spawn(move || {
            loop {
                thread::sleep(TIME_UNIT * chunk_opt.end_time.unwrap() as u32);
                swap_tx.send(chunk_opt.end_time.unwrap());
            }
        });
        Ok(db)
    }

    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        let _c = &self.current_chuck.read().unwrap();
        let res = timepoints.into_iter().map(|tp| {
            if tp.timestamp == 0 {
                return Ok(()); //skip null
            }
            _c.insert(labels.clone(), tp)
        }).filter(|res| res.is_err()).collect::<Vec<_>>();
        if !res.is_empty() {
            error!("num of error {}", res.len());
            return Err(MonolithErr::InternalErr("multiple time point cannot insert".to_string()));
        }
        Ok(())
    }

    pub fn write_time_point(&self, labels: Labels, timepoint: TimePoint) -> Result<()> {
        let _c = &self.current_chuck.read().unwrap();
        _c.insert(labels, timepoint)?;
        Ok(())
    }

    pub fn query(
        &self,
        labels: Labels,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimeSeries>> {
        let _c = &self.current_chuck.read().unwrap();
        _c.query(labels, start_time, end_time)
    }

    fn swap(&self) -> Result<()> {
        let (storage, indexer) = (self.storage_builder.build()?, self.indexer_builder.build()?);
        let current_time = get_current_timestamp();
        let chunk_opt = ChunkOpts {
            start_time: Some(current_time),
            end_time: Some(current_time + self.options.chunk_size().as_millis() as Timestamp),
        };
        let chunk = Chunk::<S, I>::new(storage, indexer, &chunk_opt);
        {
            let stale = self.current_chuck.read().unwrap();
            self.secondary_chunks.write().unwrap().push(stale.clone());
        }
        {
            let mut current = self.current_chuck.write().unwrap(); //will also block all read util swap finish
            *current = Arc::new(chunk);
        }
        Ok(())
    }
}


