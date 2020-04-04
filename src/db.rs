use crate::chunk::{Chunk, ChunkOpts};
use crate::option::DbOpts;
use crate::{Result, MonolithErr, TIME_UNIT};

use crate::storage::{Storage, SledStorage};
use std::sync::{RwLock, Arc};
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::indexer::{SledIndexer, Indexer};
use crate::common::utils::get_current_timestamp;
use std::cell::{Cell, RefCell};
use std::borrow::Borrow;
use std::ops::Deref;
use std::rc::Rc;
use std::thread;
use std::sync::mpsc::{Receiver, channel};

/// MonolithDb is thread-safe
pub struct MonolithDb<S: Storage, I: Indexer> {
    current_chuck: RwLock<Arc<Chunk<S, I>>>,
    secondary_chunks: RwLock<Vec<Arc<Chunk<S, I>>>>,
    //todo: chunk swap
    options: DbOpts,
    on_swap: Receiver<Timestamp>,
}

impl<S: Storage, I: Indexer> MonolithDb<S, I>
    where Self: NewDb {
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

    fn swap(&self, chunk: Chunk<S, I>) -> Result<()> {
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
        let (swap_tx, swap_rx) = channel::<Timestamp>();
        let chunk = Chunk::<Self::S, Self::I>::new(storage, indexer, &chunk_opt);
        let db = MonolithDb {
            current_chuck: RwLock::new(Arc::new(chunk)),
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
            on_swap: swap_rx,
        };
        thread::spawn(move || {
            loop {
                thread::sleep(TIME_UNIT * chunk_opt.end_time.unwrap() as u32);
                swap_tx.send(chunk_opt.end_time.unwrap())
            }
        });
        Ok(db)
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


//todo: use builder to replace NewDb trait
trait ChunkBuilder<S, I>
    where S: Storage,
          I: Indexer {
    fn build() -> Chunk<S, I>;
}

