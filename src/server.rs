use crate::chunk::Chunk;
use crate::option::ServerOps;
use crate::Result;
use std::env;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

pub struct MonolithServer {
    current_chuck: Chunk,
    secondary_chunks: RwLock<Vec<Chunk>>,
    options: ServerOps,
}

impl MonolithServer {
    pub fn new(ops: ServerOps) -> Result<MonolithServer> {
        Ok(MonolithServer {
            current_chuck: Chunk::new(),
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
        })
    }
}
