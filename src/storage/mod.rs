use crate::common::time_series::TimeSeriesId;
use crate::common::time_point::{Timestamp, Value, TimePoint};
use crate::common::label::Labels;
use crate::Result;

mod sled_storage;

pub use sled_storage::SledStorage;


pub trait Storage {
    fn write_time_point(&self, time_series_id: TimeSeriesId, timestamp: Timestamp, value: Value) -> Result<()>;

    fn read_time_series(&self, time_series_id: TimeSeriesId, start_time: Timestamp, end_time: Timestamp) -> Result<Vec<TimePoint>>;
}

pub trait Encoder {
    fn encode_time_point(time_stamp: Timestamp, value: Value) -> Result<String>;

    fn encode_time_series_labels(time_series_meta: Labels) -> Result<String>;
}

pub trait Decoder {
    fn decode_time_point(raw: String) -> Result<TimePoint>;
}