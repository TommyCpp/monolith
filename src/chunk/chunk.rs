use std::time::{Duration, UNIX_EPOCH};

use crate::common::label::Labels;
use crate::common::time_point::{TimePoint, Timestamp};
use crate::common::time_series::TimeSeries;
use crate::common::utils::{is_duration_overlap, get_current_timestamp};
use crate::common::IdGenerator;
use crate::{MonolithErr, Result};

use crate::storage::Storage;
use crate::MonolithErr::OutOfRangeErr;

use crate::indexer::Indexer;
use std::sync::{RwLock, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};


pub const DEFAULT_CHUNK_SIZE: Timestamp = Duration::from_secs(2 * 60 * 60).as_millis() as Timestamp;

/// ChunkOps contains all options for chunk
pub struct ChunkOpts {
    // as mil sec
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
}

impl ChunkOpts {
    fn new() -> ChunkOpts {
        ChunkOpts {
            start_time: None,
            end_time: None,
        }
    }
}

///
/// Chunk store a set of time series fallen into certain time range;
///
/// Chunk is thread safe
pub struct Chunk<S: Storage, I: Indexer> {
    storage: S,
    indexer: I,
    start_time: Timestamp,
    end_time: Timestamp,
    closed: AtomicBool,
    id_generator: IdGenerator,
    mutex: RwLock<()>
}

//todo: add meta data file for chunk, build dir for each individual chunk
impl<S: Storage, I: Indexer> Chunk<S, I> {
    pub fn new(storage: S, indexer: I, ops: &ChunkOpts) -> Self {
        let start_time = ops
            .start_time
            .unwrap_or(get_current_timestamp());
        Chunk {
            storage,
            indexer,
            start_time,
            mutex: RwLock::new(()),
            end_time: ops.end_time.unwrap_or(start_time + DEFAULT_CHUNK_SIZE),
            closed: AtomicBool::new(false),
            id_generator: IdGenerator::new(1),
        }
    }

    pub fn close(&self) {
        let _m = self.mutex.write().expect("Poisoned mutex in chunk when try to close chunk");
        self.closed.store(true, Ordering::SeqCst)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn insert(&self, labels: Labels, timepoint: TimePoint) -> Result<()> {
        let _m = self.mutex.write().expect("Poisoned mutex when try to insert into chunk");
        if !self.is_in_range(&timepoint.timestamp) {
            info!("Chunk range {}, {}; but trying to insert {}", self.start_time, self.end_time, timepoint.timestamp);
            return Err(MonolithErr::OutOfRangeErr(self.start_time, self.end_time));
        }
        let id = self.indexer.get_series_id_by_labels(labels.clone())?;
        if id.is_none() {
            //insert new series
            let new_id = self.id_generator.next();
            self.indexer.create_index(labels.clone(), new_id)?;
            self.storage
                .write_time_point(new_id, timepoint.timestamp, timepoint.value)?;
        } else {
            //only one, skip the choose process
            self.storage
                .write_time_point(id.unwrap(), timepoint.timestamp, timepoint.value);
        }

        Ok(())
    }

    pub fn query(
        &self,
        labels: Labels,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimeSeries>> {
        self.mutex.read().expect("Poisoned mutex when try to read from chunk");
        if !is_duration_overlap(self.start_time, self.end_time, start_time, end_time) {
            return Err(OutOfRangeErr(self.start_time, self.end_time));
        }
        let candidates = self.indexer.get_series_with_label_matching(labels)?;
        let mut res = Vec::new();
        for (id, metadata) in candidates {
            let data = self.storage.read_time_series(id, start_time, end_time)?;
            if data.len() == 0 {
                continue; //skip empty series
            }
            res.push(TimeSeries::from_data(id, metadata, data))
        }
        Ok(res)
    }

    pub fn is_with_range(&self, start_time: Timestamp, end_time: Timestamp) -> bool {
        is_duration_overlap(self.start_time, self.end_time, start_time, end_time)
    }

    ///start time and end time of this chunk
    pub fn start_end_time(&self) -> (Timestamp, Timestamp) {
        (self.start_time, self.end_time)
    }

    fn is_in_range(&self, timestamp: &Timestamp) -> bool {
        return self.start_time < *timestamp && self.end_time > *timestamp;
    }
}

impl<S, I> PartialEq for Chunk<S, I>
    where S: Storage, I: Indexer {
    fn eq(&self, other: &Self) -> bool {
        self.start_time == other.start_time && self.end_time == other.end_time
    }
}

//todo: more precise equal strategy
impl<S, I> Eq for Chunk<S, I>
    where S: Storage, I: Indexer{
}

impl<S, I> PartialOrd for Chunk<S, I>
    where S: Storage, I: Indexer {

    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.start_time.cmp(&other.start_time))
    }
}

impl<S, I> Ord for Chunk<S, I>
    where S: Storage, I: Indexer{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}


#[cfg(test)]
mod test {

}
