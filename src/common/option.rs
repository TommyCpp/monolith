use crate::{MonolithErr, Result, CHUNK_SIZE, FILE_DIR_ARG, STORAGE_ARG};
use clap::ArgMatches;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::env::current_dir;

#[derive(Clone)]
pub struct DbOpts {
    storage: StorageType,
    base_dir: PathBuf,
    chunk_size: Duration,
}

impl DbOpts {
    pub fn default() -> DbOpts {
        DbOpts {
            storage: StorageType::SledBackendStorage,
            base_dir: current_dir().unwrap(),
            chunk_size: Default::default(),
        }
    }

    pub fn get_config(matches: ArgMatches) -> Result<DbOpts> {
        let chunk_size_str = matches.value_of(CHUNK_SIZE).unwrap();
        let chunk_size_in_sec: u64 = String::from(chunk_size_str).parse()?;
        let config = DbOpts {
            storage: StorageType::from(matches.value_of(STORAGE_ARG).unwrap())?,
            base_dir: PathBuf::from_str(matches.value_of(FILE_DIR_ARG).unwrap())?,
            chunk_size: Duration::from_secs(chunk_size_in_sec),
        };

        Ok(config)
    }

    pub fn storage(&self) -> &StorageType {
        &self.storage
    }

    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    pub fn chunk_size(&self) -> &Duration {
        &self.chunk_size
    }
}

pub struct ServerOpts {
    port: i32,
    write_path: &'static str,
    read_path: &'static str,
    worker_num: usize
}

impl ServerOpts {
    pub fn new() {}
}

#[derive(Clone)]
pub enum StorageType {
    SledBackendStorage,
}

impl StorageType {
    pub fn from(name: &str) -> Result<StorageType> {
        match name {
            "sled" => Ok(StorageType::SledBackendStorage),
            _ => Err(MonolithErr::OptionErr),
        }
    }
}
