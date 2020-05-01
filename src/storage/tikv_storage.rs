use tikv_client::{RawClient, Config};
use crate::storage::Storage;
use crate::{Result, HasTypeName, Builder, Timestamp, Value};
use std::path::PathBuf;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeriesId;
use crate::backend::tikv::{TiKvRawBackend, TiKvRawBackendBuilder};


struct TiKvStorage {
    config: Config,
    client: Box<dyn TiKvRawBackend>,
}

impl Storage for TiKvStorage {
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

impl HasTypeName for TiKvStorage {
    fn get_type_name() -> &'static str {
        "TiKvStorage"
    }
}

struct TiKvStorageBuilder {
    backend_builder: TiKvRawBackendBuilder
}

impl TiKvStorageBuilder {
    fn new(backend_builder: TiKvRawBackendBuilder) -> Result<TiKvStorageBuilder> {
        Ok(
            TiKvStorageBuilder {
                backend_builder
            }
        )
    }
}

impl Builder<TiKvStorage> for TiKvStorageBuilder {
    fn build(&self, _: String) -> Result<TiKvStorage> {
        Ok(
            TiKvStorage {
                config: Default::default(),
                client: self.backend_builder.build("".to_string())?,
            }
        )
    }
}