use crate::{
    MonolithErr, Result, CHUNK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_PORT, DEFAULT_READ_PATH,
    DEFAULT_WORKER_NUM, DEFAULT_WRITE_PATH, FILE_DIR_ARG, INDEXER_ARG, PORT, READ_PATH,
    SLED_BACKEND, STORAGE_ARG, TIKV_CONFIG, WORKER_NUM, WRITE_PATH,
};
use clap::ArgMatches;
use std::env::current_dir;
use std::fmt::{self, Formatter};
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
        let chunk_size_str = matches.get_one::<String>(CHUNK_SIZE).unwrap();
        let chunk_size_in_sec: u64 = chunk_size_str.parse()?;
        let config = DbOpts {
            storage: matches.get_one::<String>(STORAGE_ARG).unwrap().to_string(),
            indexer: matches.get_one::<String>(INDEXER_ARG).unwrap().to_string(),
            base_dir: PathBuf::from_str(matches.get_one::<String>(FILE_DIR_ARG).unwrap())?,
            chunk_size: Duration::from_secs(chunk_size_in_sec),
            tikv_config: matches.get_one::<String>(TIKV_CONFIG).map(PathBuf::from),
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

pub struct ServerOpts {
    pub port: i32,
    pub write_path: String,
    pub read_path: String,
    pub worker_num: usize,
}

impl ServerOpts {
    pub fn get_config(matchers: &ArgMatches) -> Result<ServerOpts> {
        //unwrap matcher.get_one because all options has default value.
        let worker_num = matchers.get_one::<String>(WORKER_NUM).unwrap().parse::<usize>()?;
        let port = matchers.get_one::<String>(PORT).unwrap().parse::<i32>()?;
        if port > 65535 || port < 1024 {
            return Err(MonolithErr::OptionErr);
        };
        Ok(ServerOpts {
            port,
            worker_num,
            write_path: matchers.get_one::<String>(WRITE_PATH).unwrap().to_string(),
            read_path: matchers.get_one::<String>(READ_PATH).unwrap().to_string(),
        })
    }
}

impl fmt::Display for ServerOpts {
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

impl Default for ServerOpts {
    fn default() -> ServerOpts {
        ServerOpts {
            port: DEFAULT_PORT,
            write_path: DEFAULT_WRITE_PATH.to_string(),
            read_path: DEFAULT_READ_PATH.to_string(),
            worker_num: DEFAULT_WORKER_NUM,
        }
    }
}
