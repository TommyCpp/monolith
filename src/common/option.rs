use crate::{
    MonolithErr, Result, CHUNK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_PORT, DEFAULT_READ_PATH,
    DEFAULT_WORKER_NUM, DEFAULT_WRITE_PATH, FILE_DIR_ARG, INDEXER_ARG, PORT, READ_PATH,
    SLED_BACKEND, STORAGE_ARG, TIKV_CONFIG, WORKER_NUM, WRITE_PATH,
};
use clap::ArgMatches;
use failure::_core::fmt::Formatter;
use std::env::current_dir;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone)]
pub struct DbOpts {
    pub storage: String,
    pub indexer: String,
    pub base_dir: PathBuf,
    pub chunk_size: Duration,
    pub tikv_config: Option<PathBuf>,
}

impl DbOpts {
    pub fn get_config(matches: &ArgMatches) -> Result<DbOpts> {
        let chunk_size_str = matches.value_of(CHUNK_SIZE).unwrap();
        let chunk_size_in_sec: u64 = String::from(chunk_size_str).parse()?;
        let config = DbOpts {
            storage: matches.value_of(STORAGE_ARG).unwrap().to_string(),
            indexer: matches.value_of(INDEXER_ARG).unwrap().to_string(),
            base_dir: PathBuf::from_str(matches.value_of(FILE_DIR_ARG).unwrap())?,
            chunk_size: Duration::from_secs(chunk_size_in_sec),
            tikv_config: matches.value_of(TIKV_CONFIG).map(PathBuf::from),
        };

        Ok(config)
    }
}

impl Default for DbOpts {
    fn default() -> Self {
        DbOpts {
            storage: SLED_BACKEND.to_string(),
            indexer: SLED_BACKEND.to_string(),
            base_dir: current_dir().unwrap(),
            chunk_size: Duration::from_secs(u64::from_str(DEFAULT_CHUNK_SIZE).unwrap()),
            tikv_config: None,
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
        Ok(ServerOpts {
            port,
            worker_num,
            write_path: matchers.value_of(WRITE_PATH).unwrap(),
            read_path: matchers.value_of(READ_PATH).unwrap(),
        })
    }
}

impl<'a> fmt::Display for ServerOpts<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Server options: \n \
        port: {} \n \
        write_path: {} \n \
        read_path: {} \n \
        num of worker: {} \n ",
            self.port, self.write_path, self.read_path, self.worker_num
        )
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
