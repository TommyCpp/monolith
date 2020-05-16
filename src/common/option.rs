use crate::{MonolithErr, Result, CHUNK_SIZE, FILE_DIR_ARG, STORAGE_ARG, DEFAULT_PORT, DEFAULT_WRITE_PATH, DEFAULT_READ_PATH, DEFAULT_WORKER_NUM, WORKER_NUM, PORT, WRITE_PATH, READ_PATH, DEFAULT_CHUNK_SIZE};
use clap::ArgMatches;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::env::current_dir;
use std::fmt;
use failure::_core::fmt::Formatter;

#[derive(Clone)]
pub struct DbOpts {
    pub storage: StorageType,
    pub base_dir: PathBuf,
    pub chunk_size: Duration,
}

impl DbOpts {
    pub fn get_config(matches: &ArgMatches) -> Result<DbOpts> {
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

impl Default for DbOpts {
    fn default() -> Self {
        DbOpts {
            storage: StorageType::SledBackendStorage,
            base_dir: current_dir().unwrap(),
            chunk_size: Duration::from_secs(u64::from_str(DEFAULT_CHUNK_SIZE).unwrap()),
        }
    }
}

pub struct ServerOpts<'a> {
    pub port: i32,
    pub write_path: &'a str,
    pub read_path: &'a str,
    pub worker_num: usize,
}

impl<'a> ServerOpts<'a> {
    pub fn get_config(matchers: &'a ArgMatches) -> Result<ServerOpts<'a>> {
        //unwrap matcher.value_of because all options has default value.
        let worker_num = matchers.value_of(WORKER_NUM).unwrap().parse::<usize>()?;
        let port = matchers.value_of(PORT).unwrap().parse::<i32>()?;
        if port > 65535 || port < 1024 {
            return Err(MonolithErr::OptionErr);
        };
        Ok(
            ServerOpts {
                port,
                worker_num,
                write_path: matchers.value_of(WRITE_PATH).unwrap(),
                read_path: matchers.value_of(READ_PATH).unwrap(),
            }
        )
    }
}

impl<'a> fmt::Display for ServerOpts<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Server options: \n \
        port: {} \n \
        write_path: {} \n \
        read_path: {} \n \
        num of worker: {} \n ", self.port, self.write_path, self.read_path, self.worker_num)
    }
}

impl<'a> Default for ServerOpts<'a> {
    fn default() -> ServerOpts<'a> {
        ServerOpts {
            port: DEFAULT_PORT,
            write_path: DEFAULT_WRITE_PATH,
            read_path: DEFAULT_READ_PATH,
            worker_num: DEFAULT_WORKER_NUM,
        }
    }
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
