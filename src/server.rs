use crate::chunk::Chunk;
use crate::{Result, ServerOps};
use std::sync::RwLock;
use std::path::PathBuf;
use std::time::Duration;
use std::env;



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