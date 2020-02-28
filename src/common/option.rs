use crate::{Result, MonolithErr, STORAGE_ARG, FILE_DIR_ARG, CHUNK_SIZE};
use crate::StorageType::SledBackendStorage;
use std::path::PathBuf;
use clap::ArgMatches;
use std::str::FromStr;
use std::time::Duration;

pub struct ServerOps {
    storage: StorageType,
    base_dir: PathBuf,
    chunk_size: Duration,
}

pub enum StorageType {
    SledBackendStorage
}

impl StorageType {
    pub fn from(name: &str) -> Result<StorageType> {
        match name {
            "sled" => Ok(SledBackendStorage),
            _ => Err(MonolithErr::OptionErr)
        }
    }
}

pub fn get_config(matches: ArgMatches) -> Result<ServerOps> {
    let chunk_size_str = matches.value_of(CHUNK_SIZE).unwrap();
    let chunk_size_in_sec: u64 = String::from(chunk_size_str).parse()?;


    Ok(
        ServerOps {
            storage: StorageType::from(matches.value_of(STORAGE_ARG).unwrap())?,
            base_dir: PathBuf::from_str(matches.value_of(FILE_DIR_ARG).unwrap())?,
            chunk_size: Duration::from_secs(chunk_size_in_sec)
        }
    )
}