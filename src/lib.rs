mod common;
mod chunk;
mod server;
mod error;

pub use chunk::*;
pub use error::*;
pub use server::MonolithServer;
pub use common::option::*;

pub const STORAGE_ARG: &str = "storage";
pub const FILE_DIR_ARG: &str = "~/.monolith";