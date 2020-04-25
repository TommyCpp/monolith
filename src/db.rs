use std::{fs, thread};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;
use std::time::Duration;

use crate::{Builder, MonolithErr, Result, Timestamp, DB_METADATA_FILENAME, CHUNK_METADATA_FILENAME};
use crate::chunk::{Chunk, ChunkOpts};
use crate::common::label::Labels;
use crate::common::time_series::LabelPointPairs;
use crate::common::utils::{decode_chunk_dir, encode_chunk_dir, get_current_timestamp};
use crate::indexer::Indexer;
use crate::option::DbOpts;
use crate::storage::Storage;
use crate::common::time_point::TimePoint;
use crate::common::metadata::{DbMetadata, ChunkMetadata};
use std::fs::File;
use std::io::{BufReader, BufWriter};

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
        let db_metadata = DbMetadata {
            indexer_type: S::get_type_name().to_string(),
            storage_type: I::get_type_name().to_string(),
        };
        Self::read_or_create_metadata(&ops.base_dir(), &db_metadata);

        let chunk_size = ops.chunk_size().as_millis() as Timestamp;
        //read existing data
        let existing_chunk = Self::read_existing_chunk(&ops.base_dir())?;

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
        let chunk_dir_str = chunk_dir.as_path().display().to_string();
        let (storage, indexer) =
            (storage_builder.build(chunk_dir_str.clone())?,
             indexer_builder.build(chunk_dir_str.clone())?);
        let chunk = Chunk::<S, I>::new(storage, indexer, &chunk_opt);
        let (swap_tx, swap_rx) = channel::<Timestamp>();
        let db = Arc::new(MonolithDb {
            current_chuck: RwLock::new(Arc::new(chunk)),
            secondary_chunks: RwLock::new(existing_chunk),
            options: ops,
            storage_builder,
            indexer_builder,
        });
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

    fn read_or_create_metadata(base_dir: &Path, db_metadata: &DbMetadata) -> Result<()> {
        let metadata_file = Option::transpose(
            fs::read_dir(base_dir)?
                .find(|entry|
                    entry.is_ok() && entry.as_ref().unwrap().file_name().into_string().unwrap() == DB_METADATA_FILENAME)).unwrap();

        if metadata_file.is_some() {
            let file = File::open(metadata_file.unwrap().path())?;
            let reader = BufReader::new(file);
            let metadata: DbMetadata = serde_json::from_reader(reader)?;
            if metadata.indexer_type != db_metadata.indexer_type || metadata.storage_type != db_metadata.storage_type {
                return Err(MonolithErr::OptionErr);
            }
        } else {
            let file = File::create(base_dir.join(DB_METADATA_FILENAME))?;
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, db_metadata);
        }

        Ok(())
    }

    fn read_existing_chunk(dir: &Path) -> Result<Vec<Arc<Chunk<S, I>>>> {
        let mut res = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path_str = entry?.path().into_os_string().into_string().unwrap();
            let _slash = path_str.rfind(std::path::MAIN_SEPARATOR).unwrap() + 1;
            let dir_name = path_str.clone();
            let dir_name = dir_name.chars().skip(_slash).take(dir_name.len() - _slash).collect();
            match decode_chunk_dir(dir_name) {
                Ok((start_time, end_time)) => {
                    let storage = S::read_from_existing(PathBuf::from(path_str.clone()).join("storage"))?;
                    let indexer = I::read_from_existing(PathBuf::from(path_str.clone()).join("indexer"))?;
                    let current_time = get_current_timestamp();
                    let chunk_opt = ChunkOpts {
                        start_time: Some(start_time),
                        end_time: Some(if end_time > current_time { current_time } else { end_time }),
                    };
                    let chunk = Chunk::new(storage, indexer, &chunk_opt);
                    chunk.close();
                    (&mut res).push(Arc::new(chunk));
                }
                Err(_) => {}
            }
        }
        res.sort();

        Ok(res)
    }

    pub fn write_time_points(&self, labels: Labels, timepoints: Vec<TimePoint>) -> Result<()> {
        let _c = &self.current_chuck.read().unwrap();
        let (start_time, end_time) = _c.start_end_time();
        let res = timepoints.into_iter()
            .filter(|tp| tp.timestamp >= start_time && tp.timestamp <= end_time && tp.timestamp != 0)
            .map(|tp| {
                _c.insert(labels.clone(), tp)
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
    ) -> Result<LabelPointPairs> {
        let mut res = HashMap::<Labels, Vec<TimePoint>>::new();
        let res_ref = &mut res;
        //insert in reverse time order in order to quickly append vec
        {
            //check current chunk
            let _c = &self.current_chuck.read().unwrap();
            let _res = _c.query(labels.clone(), start_time, end_time);
            if _res.is_ok() {
                for ref t in _res.unwrap() {
                    res_ref.insert(t.meta_data().clone(), t.time_points().clone().into_iter().rev().collect());
                };
            } else {}
        };
        {
            //chuck previous chunk, normally users query more on recent data, so we go backwards
            let chunks = &self.secondary_chunks.read().unwrap();
            for _c in chunks.iter().rev() {
                if _c.is_with_range(start_time, end_time) {
                    let _res = _c.query(labels.clone(), start_time, end_time);
                    if _res.is_ok() {
                        for ref t in _res.unwrap() {
                            let mut current_val = t.time_points().clone().into_iter().rev().collect::<Vec<TimePoint>>();
                            match res_ref.get_mut(t.meta_data()) {
                                Some(val) => {
                                    val.append(&mut current_val);
                                }
                                None => {
                                    res_ref.insert(t.meta_data().clone(), current_val);
                                }
                            };
                        };
                    } else {}
                };
            }
        }
        //reverse time again before return
        let res_vec: Vec<(Labels, Vec<TimePoint>)> = res.keys()
            .map(|k| res.get_key_value(k))
            .filter(|o| o.is_some())
            .map(|o| o.unwrap())
            .map(|(labels, points)|
                (labels.clone(), points.iter().cloned().rev().collect::<Vec<TimePoint>>())
            )
            .collect();
        Ok(res_vec)
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
        let chunk_dir_str = chunk_dir.as_path().display().to_string();
        let (storage, indexer) =
            (self.storage_builder.build(chunk_dir_str.clone())?,
             self.indexer_builder.build(chunk_dir_str.clone())?);
        // write metadata into chunk
        let metadata = ChunkMetadata {
            start_time,
            end_time: chunk_opt.end_time.unwrap(),
        };
        let file = File::create(chunk_dir.as_path().join(PathBuf::from(CHUNK_METADATA_FILENAME)));
        if file.is_err(){
            error!("Cannot create metadata file for chunk");
            return Err(MonolithErr::InternalErr("Cannot create metadata file for chunk".to_string()));
        } else{
            let writer = BufWriter::new(file.unwrap());
            serde_json::to_writer(writer, &metadata);
        }
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::common::metadata::DbMetadata;
    use crate::{MonolithDb, Result, DB_METADATA_FILENAME};
    use crate::indexer::SledIndexer;
    use crate::common::test_utils::{StubStorage, StubIndexer};
    use std::path::PathBuf;
    use std::fs::File;
    use std::io::BufWriter;

    #[test]
    fn test_read_metadata() -> Result<()> {
        let tempdir = TempDir::new()?;
        let metadata = DbMetadata {
            indexer_type: "TestIndexer".to_string(),
            storage_type: "TestStorage".to_string(),
        };

        let mut res = MonolithDb::<StubStorage, StubIndexer>::read_or_create_metadata(tempdir.as_ref(), &metadata);
        assert!(res.is_ok());
        assert!(File::open(tempdir.path().join(PathBuf::from(DB_METADATA_FILENAME))).is_ok());

        let file = File::open(tempdir.path().join(PathBuf::from(DB_METADATA_FILENAME))).unwrap();
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &metadata);

        let new_metadata = DbMetadata {
            indexer_type: "StubIndexer".to_string(),
            storage_type: "StubStorage".to_string(),
        };
        res = MonolithDb::<StubStorage, StubIndexer>::read_or_create_metadata(tempdir.as_ref(), &new_metadata);
        assert!(res.is_err());

        res = MonolithDb::<StubStorage, StubIndexer>::read_or_create_metadata(tempdir.as_ref(), &metadata);
        assert!(res.is_ok());


        Ok(())
    }
}