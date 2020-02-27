use crate::{Result, MonolithErr, STORAGE_ARG, FILE_DIR_ARG};
use crate::StorageType::SledgeStorage;
use std::path::PathBuf;
use clap::ArgMatches;
use std::str::FromStr;

pub struct ServerOps {
    storage: StorageType,
    base_dir: PathBuf,
    //todo: add more options, like chunk size
}

pub enum StorageType {
    SledgeStorage
}

impl StorageType {
    pub fn from(name: &str) -> Result<StorageType> {
        match name {
            "sled" => Ok(SledgeStorage),
            _ => Err(MonolithErr::OptionErr)
        }
    }
}

pub fn get_config(matches: ArgMatches) -> Result<ServerOps> {
    Ok(
        ServerOps {
            storage: StorageType::from(matches.value_of(STORAGE_ARG).unwrap())?,
            base_dir: PathBuf::from_str(matches.value_of(FILE_DIR_ARG).unwrap())?,
        }
    )
}