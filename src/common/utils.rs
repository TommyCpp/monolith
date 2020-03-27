use crate::common::time_point::Timestamp;
use std::time::Instant;

pub fn is_duration_overlap(
    start_time_1: Timestamp,
    end_time_1: Timestamp,
    start_time_2: Timestamp,
    end_time_2: Timestamp,
) -> bool {
    return !(start_time_1 > end_time_2 || end_time_1 < start_time_2);
}

pub fn get_current_timestamp() -> Timestamp {
    Instant::now().elapsed().as_millis() as Timestamp
}