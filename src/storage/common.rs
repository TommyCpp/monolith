use crate::common::time_series::TimeSeriesId;
use crate::{Timestamp, Value, Result};
use crate::common::time_point::TimePoint;
use std::path::PathBuf;

///
/// Storage is in charge of storing time series data
/// Note that the label should be store in Indexer instead of Storage
pub trait Storage: Sized {
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

    fn read_from_existing(dir: PathBuf) -> Result<Self>;
}

pub trait Encoder {
    fn encode_time_point(time_stamp: Timestamp, value: Value) -> Result<Vec<u8>>;
}

pub trait Decoder {
    fn decode_time_point(raw: &[u8]) -> Result<TimePoint>;
}