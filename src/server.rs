use crate::Chunk;
use crate::Result;

pub struct MonolithServer {
    current_chuck: Chunk
}

impl MonolithServer {
    pub fn new() -> Result<MonolithServer> {
        Ok(MonolithServer {
            current_chuck: Chunk::new()
        })
    }
}