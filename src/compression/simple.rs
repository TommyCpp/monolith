use crate::common::time_point::TimePoint;
use crate::Timestamp;

pub fn compress(data: Vec<TimePoint>) -> Vec<u8> {}


// Use delta of delta similar with Gorilla
// But instead of use 10,110,1110,1111 to mark four different situation.
// We use one byte to mark the four situation into 0,1,2,3
pub fn compress_timestamp(n_2: Timestamp, n_1: Timestamp, n: Timestamp) -> Vec<u8> {
}