use crate::chunk::Chunk;
use crate::Result;
use std::sync::RwLock;
use std::path::PathBuf;
use std::time::Duration;
use std::env;

const DEFAULT_CHUNK_DURATION: Duration = Duration::from_secs(15 as u64);

pub struct MonolithServerOps {
    base_dir: PathBuf,
    chunk_duration: Duration,
}

impl MonolithServerOps {
    pub fn new() -> Result<MonolithServerOps> {
        Ok(MonolithServerOps {
            base_dir: env::current_dir()?,
            chunk_duration: DEFAULT_CHUNK_DURATION,
        })
    }
}

pub struct MonolithServer {
    current_chuck: Chunk,
    secondary_chunks: RwLock<Vec<Chunk>>,
    options: MonolithServerOps,
}

impl MonolithServer {
    pub fn new(ops: Option<MonolithServerOps>) -> Result<MonolithServer> {
        Ok(MonolithServer {
            current_chuck: Chunk::new(),
            secondary_chunks: RwLock::new(Vec::new()),
            options: match ops {
                Some(op) => op,
                None => MonolithServerOps::new()?
            },
        })
    }
}