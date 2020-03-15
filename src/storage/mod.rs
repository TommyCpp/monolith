use crate::common::time_point::{TimePoint, Timestamp, Value};
use crate::common::time_series::TimeSeriesId;
use crate::Result;

mod sled_storage;

pub use sled_storage::SledStorage;

///
/// Storage is in charge of storing time series data
/// Note that the label should be store in Indexer instead of Storage
pub trait Storage {
    fn write_time_point(
        &self,
        time_series_id: TimeSeriesId,
        timestamp: Timestamp,
        value: Value,
    ) -> Result<()>;

    fn read_time_series(
        &self,
        time_series_id: TimeSeriesId,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimePoint>>;
}

pub trait Encoder {
    fn encode_time_point(time_stamp: Timestamp, value: Value) -> Result<String>;
}

pub trait Decoder {
    fn decode_time_point(raw: String) -> Result<TimePoint>;
}
