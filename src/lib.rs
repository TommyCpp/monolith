mod common;
mod error;

pub mod chunk;
pub mod db;
pub mod server;
pub mod indexer;
pub mod storage;
mod backend;
/// Generated proto definition
pub(crate) mod proto;

pub use common::*;
pub use error::*;
pub use db::MonolithDb;
use std::time::Duration;

pub type Timestamp = u64;
pub type Value = f64;

// cli option name
pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "file_dir";
pub const CHUNK_SIZE: &str = "chunk_size";
pub const PORT: &str = "port";
pub const READ_PATH: &str = "read_path";
pub const WRITE_PATH: &str = "write_path";
pub const WORKER_NUM: &str = "worker_num";


pub const TIME_UNIT: Duration = Duration::from_micros(1);

// cli option default value
pub const DEFAULT_CHUNK_SIZE: &str = "12000"; //in seconds
pub const DEFAULT_PORT: i32 = 9001;
pub const DEFAULT_READ_PATH: &str = "/read";
pub const DEFAULT_WRITE_PATH: &str = "/write";
pub const DEFAULT_WORKER_NUM: usize = 8;

pub const DB_METADATA_FILENAME: &'static str = "metadata.json";
pub const CHUNK_METADATA_FILENAME: &'static str = "metadata.json";


#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
