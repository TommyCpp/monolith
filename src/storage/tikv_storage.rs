use tikv_client::{RawClient, Config};
use crate::storage::Storage;
use crate::{Result, HasTypeName, Builder, Timestamp, Value, MonolithErr};
use std::path::{PathBuf, Path};
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeriesId;
use crate::backend::tikv::{TiKvRawBackend, TiKvRawBackendSingleton};
use crate::common::option::DbOpts;
use crate::chunk::{ChunkOpts, Chunk};

/// Storage that uses shared Tikv backend.
///
struct TiKvStorage {
    client: Box<dyn TiKvRawBackend>,
    chunk_identifier: Vec<u8>,
    storage_identifier: Vec<u8>,
}

impl TiKvStorage{
}

impl Storage for TiKvStorage {
    fn write_time_point(&self, time_series_id: TimeSeriesId, timestamp: Timestamp, value: Value) -> Result<()> {
        unimplemented!()
    }

    fn read_time_series(&self, time_series_id: TimeSeriesId, start_time: Timestamp, end_time: Timestamp) -> Result<Vec<TimePoint>> {
        unimplemented!()
    }
}

impl HasTypeName for TiKvStorage {
    fn get_type_name() -> &'static str {
        "TiKvStorage"
    }
}

struct TiKvStorageBuilder {
    backend_builder: TiKvRawBackendSingleton
}

impl TiKvStorageBuilder {
    fn new(backend_builder: TiKvRawBackendSingleton) -> Result<TiKvStorageBuilder> {
        Ok(
            TiKvStorageBuilder {
                backend_builder
            }
        )
    }
}

impl Builder<TiKvStorage> for TiKvStorageBuilder {
    fn build(&self, _: String, chunk_opts: Option<&ChunkOpts>, _: Option<&DbOpts>) -> Result<TiKvStorage> {
        let client = self.backend_builder.get_instance()?;
        let chunk_identifier = chunk_opts.ok_or(MonolithErr::OptionErr)?.identifier.clone();
        let mut instance = TiKvStorage {
            storage_identifier: client.init_component(chunk_identifier.clone(), false)?,
            client,
            chunk_identifier,
        };
        Ok(
            instance
        )
    }

    fn write_to_chunk(&self, dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_from_chunk(&self, dir: &Path) -> Result<Option<TiKvStorage>> {
        let chunk_opts = ChunkOpts::from_dir(dir)?;
        let client = self.backend_builder.get_instance()?;
        if let Some(val) = client.get(chunk_opts.identifier.clone())?{
            let storage_identifier = Vec::from(&val[16..]);
            return Ok(
                Some(
                    TiKvStorage{
                        client,
                        chunk_identifier: chunk_opts.identifier,
                        storage_identifier
                    }
                )
            )
        }

        Ok(None)
    }

    fn write_config(&self, dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_config(&self, dir: &Path) -> Result<()> {
        Ok(())
    }
}