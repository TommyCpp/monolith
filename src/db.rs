use crate::chunk::{Chunk, ChunkOpts};
use crate::option::DbOpts;
use crate::{Result, MonolithErr, TIME_UNIT, Builder};

use crate::storage::{Storage, SledStorage};
use std::sync::{RwLock, Arc, Mutex};
use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::indexer::{SledIndexer, Indexer};
use crate::common::utils::{get_current_timestamp, encode_chunk_dir};
use std::cell::{Cell, RefCell};
use std::thread;
use std::sync::mpsc::{Receiver, channel, Sender};
use std::borrow::BorrowMut;
use std::path::PathBuf;
use crate::MonolithErr::OutOfRangeErr;
use failure::Fail;
use std::any::Any;
use std::time::Duration;
use std::collections::HashMap;

/// MonolithDb is thread-safe
pub struct MonolithDb<S: Storage, I: Indexer>
    where S: Storage + Send + Sync,
          I: Indexer + Send + Sync {
    current_chuck: RwLock<Arc<Chunk<S, I>>>,
    secondary_chunks: RwLock<Vec<Arc<Chunk<S, I>>>>,
    options: DbOpts,
    storage_builder: Box<dyn Builder<S> + Sync + Send>,
    indexer_builder: Box<dyn Builder<I> + Sync + Send>,
}

impl<S, I> MonolithDb<S, I>
    where S: Storage + Send + Sync + 'static,
          I: Indexer + Send + Sync + 'static {
    pub fn new(ops: DbOpts, storage_builder: Box<dyn Builder<S> + Sync + Send>, indexer_builder: Box<dyn Builder<I> + Sync + Send>) -> Result<Arc<Self>> {
        let chunk_size = ops.chunk_size().as_millis() as Timestamp;
        let current_time = get_current_timestamp();
        let chunk_opt = ChunkOpts {
            start_time: Some(current_time),
            end_time: Some(current_time + ops.chunk_size().as_millis() as Timestamp),
        };
        let chunk_dir = ops.base_dir()
            .as_path()
            .join(
                encode_chunk_dir(chunk_opt.start_time.unwrap(),
                                 chunk_opt.end_time.unwrap()));
        let (storage, indexer) =
            (storage_builder.build(chunk_dir.clone())?,
             indexer_builder.build(chunk_dir.clone())?);
        let chunk = Chunk::<S, I>::new(storage, indexer, &chunk_opt);
        let (swap_tx, swap_rx) = channel::<Timestamp>();
        let db = Arc::new(MonolithDb {
            current_chuck: RwLock::new(Arc::new(chunk)),
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
            storage_builder,
            indexer_builder,
        });
        //TODO: test chunk swap with prom, still get out of range error sometimes.
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(chunk_size));
                swap_tx.send(get_current_timestamp());
            }
        });
        let _db = db.clone();
        thread::spawn(move || {
            loop {
                let start_time = swap_rx.recv().unwrap() + 1;
                _db.clone().swap(start_time);
            }
        });
        Ok(db.clone())
    }

    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        let _c = &self.current_chuck.read().unwrap();
        let res = timepoints.into_iter().map(|tp| {
            if tp.timestamp == 0 {
                return Ok(()); //skip null
            }
            let res = _c.insert(labels.clone(), tp);
            if res.is_err() {
                let err = res.err().unwrap();
                return if let MonolithErr::OutOfRangeErr(st, et) = err {
                    // skip out of range error
                    info!("one out of range error with target range {}, {}", st, et);
                    Ok(())
                } else {
                    Err(err)
                };
            }
            Ok(())
        }).filter(|res| res.is_err()).collect::<Vec<_>>();
        if !res.is_empty() {
            error!("{} time point failed to insert", res.len());
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
        let mut res = HashMap::<Labels, Vec<TimePoint>>::new();
        let res_ref = &mut res;
        {
            //check current chunk
            let _c = &self.current_chuck.read().unwrap();
            let _res = _c.query(labels, start_time, end_time);
            if _res.is_ok() {
                _res.unwrap().iter_mut().map(|t| {
                   //todo: insert label, timepoint vec pair
                });
            } else {}
        };

        Ok(res.values().cloned().collect::<Vec<TimeSeries>>())
    }

    fn swap(&self, start_time: Timestamp) -> Result<()> {
        info!("Chunk swap, new chunk with start time {}", start_time);
        let chunk_opt = ChunkOpts {
            start_time: Some(start_time),
            end_time: Some(start_time + self.options.chunk_size().as_millis() as Timestamp),
        };

        let chunk_dir = self.options.base_dir()
            .join(
                encode_chunk_dir(chunk_opt.start_time.unwrap(), chunk_opt.end_time.unwrap())
            );
        let (storage, indexer) =
            (self.storage_builder.build(chunk_dir.clone())?,
             self.indexer_builder.build(chunk_dir.clone())?);
        let chunk = Chunk::<S, I>::new(storage, indexer, &chunk_opt);
        {
            let stale = self.current_chuck.read().unwrap();
            self.secondary_chunks.write().unwrap().push(stale.clone());
        }
        {
            let mut current = self.current_chuck.write().unwrap(); //will also block all read util swap finish
            let (_, end_time) = current.start_end_time();
            info!("The old chunk with end time {} is closing", end_time);
            current.close(); //close the stale one
            *current = Arc::new(chunk); //crate new one
        }
        Ok(())
    }
}