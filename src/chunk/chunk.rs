use crate::common::label::Labels;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeries;
use crate::common::utils::{is_duration_overlap, get_current_timestamp};
use crate::common::IdGenerator;
use crate::{MonolithErr, Result, DEFAULT_CHUNK_SIZE, Timestamp, CHUNK_METADATA_FILENAME};

use crate::storage::Storage;
use crate::MonolithErr::OutOfRangeErr;

use crate::indexer::Indexer;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::{Path, PathBuf};

use std::io::BufWriter;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::fs;

/// ChunkOps contains all options for chunk
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ChunkOpts {
    // as mil sec
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    pub identifier: Vec<u8>,
}

impl ChunkOpts {
    pub fn read_config_from_dir(dir: &Path) -> Result<Self> {
        let content = fs::read_to_string(dir.join(CHUNK_METADATA_FILENAME));
        return if content.is_ok() {
            let opts: ChunkOpts = serde_json::from_str(content.unwrap().as_str())?;
            Ok(opts)
        } else {
            error!("Cannot open option file from dir {}", dir.as_os_str().to_str().unwrap_or("<unreadable path>"));
            Err(MonolithErr::NotFoundErr)
        };
    }

    /// Write config file into dir
    pub fn write_config_to_dir(&self, dir: &Path) -> Result<()> {
        // only write when start time and end time is not null
        if self.start_time.is_some() && self.end_time.is_some() {
            std::fs::create_dir_all(dir)?;
            let file = File::create(dir.join(PathBuf::from(CHUNK_METADATA_FILENAME)))?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, &self.clone());
        }
        Ok(())
    }
}

impl Default for ChunkOpts {
    fn default() -> Self {
        ChunkOpts {
            start_time: None,
            end_time: None,
            identifier: uuid::Uuid::new_v4().as_bytes().to_vec(),
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
    mutex: RwLock<()>,
}

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
            end_time: ops.end_time.unwrap_or(start_time + DEFAULT_CHUNK_SIZE.parse::<Timestamp>().unwrap()),
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
        let candidates = self.indexer.get_series_metadata_contains_labels(labels)?;
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
    where S: Storage, I: Indexer {}

impl<S, I> PartialOrd for Chunk<S, I>
    where S: Storage, I: Indexer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.start_time.cmp(&other.start_time))
    }
}

impl<S, I> Ord for Chunk<S, I>
    where S: Storage, I: Indexer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}


#[cfg(test)]
mod test {
    use tempfile::TempDir;
    use crate::chunk::ChunkOpts;
    use crate::{Result, CHUNK_METADATA_FILENAME};
    use std::path::PathBuf;
    use crate::common::utils::encode_chunk_dir;
    use std::fs;
    use std::io::{BufWriter, Write};
    use std::fs::File;

    #[test]
    pub fn test_write_config_to_dir() -> Result<()> {
        let dir = TempDir::new()?;
        let opts = ChunkOpts::default();
        let result = opts.write_config_to_dir(dir.as_ref());
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    pub fn test_read_config_from_dir() -> Result<()> {
        let dir = TempDir::new()?;
        let opts = ChunkOpts::default();
        // Create file
        let file = File::create(dir.as_ref().join(PathBuf::from(CHUNK_METADATA_FILENAME)))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &opts)?;
        writer.flush()?;
        let result = ChunkOpts::read_config_from_dir(dir.as_ref());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), opts);

        Ok(())
    }
}
