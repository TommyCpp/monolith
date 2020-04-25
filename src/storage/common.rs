use crate::common::time_series::TimeSeriesId;
use crate::{Timestamp, Value, Result, HasTypeName};
use crate::common::time_point::TimePoint;
use std::path::PathBuf;

///
/// Storage is in charge of storing time series data
/// Note that the label should be store in Indexer instead of Storage
pub trait Storage: Sized + HasTypeName{
    /// write time point into series
    ///
    /// If a series is already present in storage, the time point will be appended.
    ///
    /// If a series is not found in storage, a new series will be created.
    fn write_time_point(
        &self,
        time_series_id: TimeSeriesId,
        timestamp: Timestamp,
        value: Value,
    ) -> Result<()>;

    /// Read time series from storage
    ///
    /// If no such time series found, then a NotFoundErr will be return/
    fn read_time_series(
        &self,
        time_series_id: TimeSeriesId,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimePoint>>;

    /// Read data from an existing dir
    fn read_from_existing(dir: PathBuf) -> Result<Self>;
}

pub trait Encoder {
    fn encode_time_point(time_stamp: Timestamp, value: Value) -> Result<Vec<u8>>;
}

pub trait Decoder {
    fn decode_time_point(raw: &[u8]) -> Result<TimePoint>;
}