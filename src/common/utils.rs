use crate::common::time_point::Timestamp;

pub fn is_duration_overlap(
    start_time_1: Timestamp,
    end_time_1: Timestamp,
    start_time_2: Timestamp,
    end_time_2: Timestamp,
) -> bool {
    return !(start_time_1 > end_time_2 || end_time_1 < start_time_2);
}
