use time_point::Timestamp;

use crate::chunk::Chunk;
use crate::label::Labels;
use crate::time_point::{TimePoint, Value};

pub mod time_point;
pub mod chunk;
pub mod time_series;
pub mod label;

fn main() {
    let t: TimePoint = TimePoint::new(12, 12.0);
}


pub struct Server {
    current_chuck: Chunk
}

impl Server {
    pub fn new() -> Server {
        Server {
            current_chuck: Chunk::new()
        }
    }

    pub fn insert(&mut self, timestamp: Timestamp, value: Value, meta_data: Labels) {
    }
}