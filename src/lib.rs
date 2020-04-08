mod common;
mod error;

pub mod chunk;
pub mod db;
pub mod server;
pub mod indexer;
pub mod storage;
/// Generated proto definition
pub(crate) mod proto;

pub use common::*;
pub use error::*;
pub use db::MonolithDb;
use std::time::Duration;


pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "file_dir";
pub const CHUNK_SIZE: &str = "chunk_size";
pub const TIME_UNIT: Duration = Duration::from_micros(1);

//in seconds, used in default value field of cli, so has to be str
pub const DEFAULT_CHUNK_SIZE: &str = "120";

#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
