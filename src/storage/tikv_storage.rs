use tikv_client::{RawClient, Config};
use crate::storage::Storage;
use crate::{Result, HasTypeName, Builder, Timestamp, Value};
use std::path::PathBuf;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeriesId;


struct TiKvStorage{
    config: Config,
    client: RawClient
}

impl Storage for TiKvStorage{
    fn write_time_point(&self, time_series_id: TimeSeriesId, timestamp: Timestamp, value: Value) -> Result<()> {
        unimplemented!()
    }

    fn read_time_series(&self, time_series_id: TimeSeriesId, start_time: Timestamp, end_time: Timestamp) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }

    fn read_from_existing(dir: PathBuf) -> Result<Self> {
        unimplemented!()
    }
}

impl HasTypeName for TiKvStorage{
    fn get_type_name() -> &'static str {
        unimplemented!()
    }
}

impl Builder<TiKvStorage> for TiKvStorage{
    fn build(&self, path: String) -> Result<TiKvStorage> {
        unimplemented!()
    }
}