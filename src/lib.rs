mod common;
mod error;

pub mod chunk;
pub mod db;
pub mod server;
pub mod indexer;
pub mod storage;
pub(crate) mod proto;

pub use common::*;
pub use error::*;
pub use db::MonolithDb;


pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "file_dir";
pub const CHUNK_SIZE: &str = "chunk_size";

//in seconds, used in default value field of cli, so has to be str
pub const DEFAULT_CHUNK_SIZE: &str = "1685513810";

#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
