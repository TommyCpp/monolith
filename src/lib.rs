mod chunk;
mod common;
mod error;
mod indexer;
mod server;

pub mod storage;

pub use indexer::common::*;
pub use chunk::*;
pub use common::*;
pub use error::*;
pub use server::MonolithServer;
use std::time::Duration;

pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "~/.monolith";
pub const CHUNK_SIZE: &str = "chunk_size";

pub const DEFAULT_CHUNK_SIZE: &str = "1200";
