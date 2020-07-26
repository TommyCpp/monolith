use crate::backend::tikv::{TiKvRawBackend, TiKvRawBackendSingleton};
use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;
use crate::common::time_point::TimePoint;
use crate::common::time_series::TimeSeriesId;
use crate::storage::sled_storage::KvStorageProcessor;
use crate::storage::{Decoder, Encoder, Storage};
use crate::{Builder, HasTypeName, MonolithErr, Result, Timestamp, Value};
use std::path::Path;
use std::path::PathBuf;

/// Storage that uses shared Tikv backend.
///
pub struct TiKvStorage {
    client: Box<dyn TiKvRawBackend>,
    chunk_identifier: Vec<u8>,
    storage_identifier: Vec<u8>,
}

impl TiKvStorage {}

impl Storage for TiKvStorage {
    fn write_time_point(
        &self,
        time_series_id: TimeSeriesId,
        timestamp: Timestamp,
        value: Value,
    ) -> Result<()> {
        let timepoint_size = std::mem::size_of::<Timestamp>() + std::mem::size_of::<Value>();
        let mut current_value = KvStorageProcessor::encode_time_point(timestamp, value)?;

        let mut key = self.storage_identifier.clone();
        let mut time_series_id_bytes: Vec<u8> = Vec::from(&time_series_id.to_be_bytes()[..]);
        key.append(&mut time_series_id_bytes);
        if let Some(mut val) = self.client.get(key.clone())? {
            //if has previous timestamp
            let last_time_point =
                KvStorageProcessor::decode_time_point(&val[(val.len() - timepoint_size)..])?;
            if last_time_point.timestamp >= timestamp {
                return Err(MonolithErr::InternalErr(
                    "Try to store a earlier time stamp".into(),
                ));
            } else {
                val.append(&mut current_value);
                self.client.set(key, val)?;
            }
        } else {
            self.client.set(key, current_value)?;
        }
        Ok(())
    }

    fn read_time_series(
        &self,
        time_series_id: TimeSeriesId,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<TimePoint>> {
        let timepoint_size = std::mem::size_of::<Timestamp>() + std::mem::size_of::<Value>();
        let mut key = self.storage_identifier.clone();
        let mut time_series_id_bytes: Vec<u8> = Vec::from(&time_series_id.to_be_bytes()[..]);
        key.append(&mut time_series_id_bytes);

        if let Some(val) = self.client.get(key.clone())? {
            let mut series: Vec<TimePoint> = Vec::new();
            for timepoint_bytes in val.chunks(timepoint_size) {
                series.push(KvStorageProcessor::decode_time_point(timepoint_bytes)?);
            }

            if series.first().unwrap().timestamp > end_time
                || series.last().unwrap().timestamp < start_time
            {
                return Err(MonolithErr::OutOfRangeErr(
                    series.first().unwrap().timestamp,
                    series.last().unwrap().timestamp,
                ));
            }

            Self::trim_time_series(series, start_time, end_time)
        } else {
            Err(MonolithErr::NotFoundErr)
        }
    }
}

impl HasTypeName for TiKvStorage {
    fn get_type_name() -> &'static str {
        "TiKvStorage"
    }
}

pub struct TiKvStorageBuilder {
    backend_builder: TiKvRawBackendSingleton,
}

impl TiKvStorageBuilder {
    pub fn new(backend_builder: TiKvRawBackendSingleton) -> Result<TiKvStorageBuilder> {
        Ok(TiKvStorageBuilder { backend_builder })
    }
}

impl Builder<TiKvStorage> for TiKvStorageBuilder {
    fn build(
        &self,
        path: String,
        chunk_opts: Option<&ChunkOpts>,
        _: Option<&DbOpts>,
    ) -> Result<TiKvStorage> {
        std::fs::create_dir_all(PathBuf::from(path).join("storage"))?;
        let client = self.backend_builder.get_instance()?;
        let chunk_identifier = chunk_opts.ok_or(MonolithErr::OptionErr)?.identifier.clone();
        let instance = TiKvStorage {
            storage_identifier: client.init_component(chunk_identifier.clone(), false)?,
            client,
            chunk_identifier,
        };
        Ok(instance)
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_from_chunk(
        &self,
        dir: &Path,
        chunk_opts: Option<&ChunkOpts>,
    ) -> Result<Option<TiKvStorage>> {
        if chunk_opts.is_none() {
            return Ok(None);
        }
        let client = self.backend_builder.get_instance()?;
        if let Some(val) = client.get(chunk_opts.unwrap().identifier.clone())? {
            let storage_identifier = Vec::from(&val[16..]);
            return Ok(Some(TiKvStorage {
                client,
                chunk_identifier: chunk_opts.unwrap().identifier.clone(),
                storage_identifier,
            }));
        }

        Ok(None)
    }

    fn write_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::tikv::TiKvRawBackend;
    use crate::common::test_utils::DummyTiKvBackend;
    use crate::storage::sled_storage::KvStorageProcessor;
    use crate::storage::tikv_storage::TiKvStorage;
    use crate::storage::{Encoder, Storage};
    use crate::Result;

    #[test]
    fn test_write_timestamp() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let storage = TiKvStorage {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            storage_identifier: "storage".to_string().into_bytes(),
        };

        // insert first time point
        storage.write_time_point(1u64, 123u64, 56.7)?;
        let mut key = storage.storage_identifier.clone();
        let mut time_series_bytes: Vec<u8> = Vec::from(&1u64.to_be_bytes()[..]);
        key.append(&mut time_series_bytes);
        let mut val = dummy_backend.get(key.clone())?;
        assert!(val.is_some());
        assert_eq!(
            val.unwrap(),
            KvStorageProcessor::encode_time_point(123u64, 56.7)?
        );

        // insert second time point
        storage.write_time_point(1u64, 124u64, 66.6)?;
        val = dummy_backend.get(key.clone())?;
        assert!(val.is_some());
        let mut first_entry = KvStorageProcessor::encode_time_point(123u64, 56.7)?;
        let mut second_entry = KvStorageProcessor::encode_time_point(124u64, 66.6)?;
        first_entry.append(&mut second_entry);
        assert_eq!(val.unwrap(), first_entry);

        // insert invalid time point, who has smaller timestamp the the previous one.
        let res = storage.write_time_point(1u64, 120u64, 120.0);
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_read_time_series() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let storage = TiKvStorage {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            storage_identifier: "storage".to_string().into_bytes(),
        };

        let mut key = storage.storage_identifier.clone();
        let mut time_series_bytes: Vec<u8> = Vec::from(&1u64.to_be_bytes()[..]);
        key.append(&mut time_series_bytes);

        let mut val = vec![];
        for (ts, v) in vec![(120u64, 12.0), (123, 16.7), (156, 89.0), (190, 10.0)] {
            val.append(&mut Vec::from(KvStorageProcessor::encode_time_point(
                ts, v,
            )?));
        }
        dummy_backend.set(key.clone(), val.clone());

        let res = storage.read_time_series(1, 120, 160)?;
        let res_flat = res
            .iter()
            .map(|tp| (tp.timestamp.clone(), tp.value.clone()))
            .collect::<Vec<(u64, f64)>>();
        assert_eq!(res_flat, vec![(120u64, 12.0), (123, 16.7), (156, 89.0)]);

        Ok(())
    }
}
