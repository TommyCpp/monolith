use crate::chunk::Chunk;
use crate::option::ServerOps;
use crate::{Result, Indexer};

use std::sync::RwLock;
use crate::storage::Storage;

pub struct MonolithServer<S: Storage, I: Indexer> {
    current_chuck: Option<Chunk<S, I>>,
    secondary_chunks: RwLock<Vec<Chunk<S, I>>>,
    options: ServerOps,
}

impl<S: Storage, I: Indexer> MonolithServer<S, I> {
    pub fn new(ops: ServerOps) -> Result<MonolithServer<S, I>> {
        Ok(MonolithServer {
            current_chuck: None,
            secondary_chunks: RwLock::new(Vec::new()),
            options: ops,
        })
    }
}
