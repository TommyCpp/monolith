use crate::common::time_series::TimeSeriesId;
use crate::common::time_point::{Timestamp, Value};

mod sled_storage;

trait Storage {
    fn write_time_point(time_series_id: TimeSeriesId, timestamp: Timestamp, value: Value);

    fn read_time_series(time_series_id: TimeSeriesId, start_time: Timestamp, end_time: Timestamp);


}
