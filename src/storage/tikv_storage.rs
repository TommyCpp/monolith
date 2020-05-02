use tikv_client::{RawClient, Config};
use crate::storage::Storage;
use crate::{Result, HasTypeName, Builder, Timestamp, Value, MonolithErr};
use std::path::PathBuf;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeriesId;
use crate::backend::tikv::{TiKvRawBackend, TakeBackendSingleton};
use crate::common::option::DbOpts;
use crate::chunk::{ChunkOpts, Chunk};

/// Storage that uses shared Tikv backend.
///
struct TiKvStorage {
    client: Box<dyn TiKvRawBackend>,
    chunk_identifier: Vec<u8>,
}

impl TiKvStorage{
    fn init(&self) -> Result<()>{
        unimplemented!()
    }
}

impl Storage for TiKvStorage {
    fn write_time_point(&self, time_series_id: TimeSeriesId, timestamp: Timestamp, value: Value) -> Result<()> {
        unimplemented!()
    }

    fn read_time_series(&self, time_series_id: TimeSeriesId, start_time: Timestamp, end_time: Timestamp) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }

    fn read_from_existing(dir: PathBuf) -> Result<Self> {
        let opts = ChunkOpts::from_dir(dir.as_path())?;
        unimplemented!()
    }
}

impl HasTypeName for TiKvStorage {
    fn get_type_name() -> &'static str {
        "TiKvStorage"
    }
}

struct TiKvStorageBuilder {
    backend_builder: TakeBackendSingleton
}

impl TiKvStorageBuilder {
    fn new(backend_builder: TakeBackendSingleton) -> Result<TiKvStorageBuilder> {
        Ok(
            TiKvStorageBuilder {
                backend_builder
            }
        )
    }
}

impl Builder<TiKvStorage> for TiKvStorageBuilder {
    fn build(&self, _: String, chunk_opts: Option<&ChunkOpts>, _: Option<&DbOpts>) -> Result<TiKvStorage> {
        let instance = TiKvStorage {
            client: self.backend_builder.get_instance()?,
            chunk_identifier: chunk_opts.ok_or(MonolithErr::OptionErr)?.identifier.clone()
        };
        instance.init();
        Ok(
            instance
        )
    }
}