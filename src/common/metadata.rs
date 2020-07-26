use crate::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DbMetadata {
    pub indexer_type: String,
    pub storage_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct ChunkMetadata {
    pub start_time: Timestamp,
    pub end_time: Timestamp,
}
