mod chunk;
mod common;
mod error;
mod db;

pub mod server;
pub mod indexer;
pub mod storage;
pub(crate) mod proto;

pub use chunk::*;
pub use common::*;
pub use error::*;
pub use indexer::common::*;
pub use db::MonolithDb;

//todo: figure out visibility between mods

pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "file_dir";
pub const CHUNK_SIZE: &str = "chunk_size";

pub const DEFAULT_CHUNK_SIZE: &str = "1200";
