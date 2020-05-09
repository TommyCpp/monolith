use crate::{Builder, Result, TiKvRawBackendSingleton, HasTypeName, MonolithErr};
use std::path::Path;
use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;
use crate::indexer::Indexer;
use crate::common::label::Labels;
use crate::backend::tikv::TiKvRawBackend;
use crate::indexer::sled_indexer::KvIndexerProcessor;
use crate::common::time_series::TimeSeriesId;
use std::convert::TryInto;
use crate::common::utils::intersect_time_series_id_vec;

const LABEL_REVERSE_PREFIX: &str = "LR";
const LABEL_PREFIX: &str = "L";
const ID_PREFIX: &str = "I";

pub struct TiKvIndexer {
    client: Box<dyn TiKvRawBackend>,
    chunk_identifier: Vec<u8>,
    indexer_identifier: Vec<u8>,
}

impl TiKvIndexer {
    fn add_indexer_id(&self, key: &mut Vec<u8>) -> Vec<u8> {
        let mut res = self.indexer_identifier.clone();
        res.append(key);
        res
    }
}

impl Indexer for TiKvIndexer {
    fn get_series_metadata_contains_labels(&self, labels: Labels) -> Result<Vec<(TimeSeriesId, Labels)>> {
        let ids = self.get_series_id_contains_labels(labels.clone())?;

        //todo: refactor this.
        Ok(
            ids.iter()
                // Convert into keys
                .map(|id|
                    (*id, self.add_indexer_id(
                        KvIndexerProcessor::encode_time_series_id(*id).into_bytes().as_mut())))
                // Get raw value from db store
                .map(|(id, key)|
                    (id, self.client.get(key)))
                // Choose the valid one
                .filter(|(id, res)|
                    res.is_ok())
                // Unwrap the valid one
                .map(|(id, res)|
                    (id, res.unwrap()))
                // Choose the one with value
                .filter(|(id, option)| option.is_some())
                // Unwrap value
                .map(|(id, option)|
                    (id, option.unwrap()))
                // Convert Vec<u8> into String
                .map(|(id, raw)|
                    (id, String::from_utf8_lossy(raw.as_slice()).to_string()))
                // Convert String into labels
                .map(|(id, raw)|
                    (id, KvIndexerProcessor::decode_labels(raw, false)))
                // Choose valid one
                .filter(|(id, res)| res.is_ok())
                // Unwrap valid one
                .map(|(id, res)|
                    (id, res.unwrap()))
                .collect()
        )
    }

    fn get_series_id_contains_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>> {
        // Get raw value from tikv
        let query_res = labels.vec().into_iter()
            .map(|label|
                self.add_indexer_id(KvIndexerProcessor::encode_label(label).into_bytes().as_mut()))
            .map(|key|
                self.client.get(key)) //todo: error handling
            .filter(|res| res.is_ok())
            .map(|res| res.unwrap())
            .filter(|res| res.is_some())
            .map(|res| res.unwrap())
            .collect::<Vec<Vec<u8>>>();
        // convert into time series id
        let mut time_series_ids: Vec<Vec<TimeSeriesId>> = vec![];
        for val in query_res {
            let ids = val.chunks(std::mem::size_of::<TimeSeriesId>())
                .map(|raw| {
                    //todo: error handling for try into
                    TimeSeriesId::from_be_bytes(raw.try_into().unwrap())
                }).collect::<Vec<TimeSeriesId>>();
            time_series_ids.push(ids);
        }
        Ok(intersect_time_series_id_vec(time_series_ids)?)
    }

    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<TimeSeriesId>> {
        let mut key = KvIndexerProcessor::encode_labels(&labels, true);
        Ok(
            self.client
                .get(self.add_indexer_id(key.into_bytes().as_mut()))?
                .and_then(
                    |v| Some(TimeSeriesId::from_be_bytes(v.as_slice().try_into().unwrap())))
        )
    }

    fn create_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()> {
        let indexer_id_str: String = String::from_utf8_lossy(self.indexer_identifier.clone().as_slice()).into();

        {
            //Create Time series id to label map
            let key = KvIndexerProcessor::encode_time_series_id(time_series_id);
            self.client.set(self.add_indexer_id(key.into_bytes().as_mut()),
                            KvIndexerProcessor::encode_labels(&labels, false).into_bytes());
        }

        {
            //Create map between full label set to time series id
            let mut key = KvIndexerProcessor::encode_labels(&labels, true);
            self.client.set(self.add_indexer_id(key.into_bytes().as_mut()),
                            Vec::from(&time_series_id.to_be_bytes()[..]));
        }

        {
            // Create Reverse Search
            labels.vec().iter()
                .map(|label|
                    self.add_indexer_id(KvIndexerProcessor::encode_label(label).into_bytes().as_mut()))
                .map(|key| (key.clone(), self.client.get(key.to_vec())))
                .filter(|(key, res)| res.is_ok())
                .map(|(key, res)| {
                    match res.unwrap() {
                        Some(mut val) => {
                            val.append(Vec::from(&time_series_id.to_be_bytes()[..]).as_mut());
                            self.client.set(key.clone(), val);
                        }
                        None => {
                            self.client.set(key.clone(),
                                            Vec::from(&time_series_id.to_be_bytes()[..]));
                        }
                    }
                })
                .collect::<Vec<()>>();
        }


        Ok(())
    }
}

impl HasTypeName for TiKvIndexer {
    fn get_type_name() -> &'static str {
        "TiKvIndexer"
    }
}

pub struct TiKvIndexerBuilder {
    backend_builder: TiKvRawBackendSingleton
}

impl TiKvIndexerBuilder {
    pub fn new(backend_builder: TiKvRawBackendSingleton) -> Result<TiKvIndexerBuilder> {
        Ok(
            TiKvIndexerBuilder {
                backend_builder,
            }
        )
    }
}

impl Builder<TiKvIndexer> for TiKvIndexerBuilder {
    fn build(&self, _: String, chunk_opts: Option<&ChunkOpts>, _: Option<&DbOpts>) -> Result<TiKvIndexer> {
        let client = self.backend_builder.get_instance()?;
        let chunk_identifier = chunk_opts.ok_or(MonolithErr::OptionErr)?.identifier.clone();
        let mut instance = TiKvIndexer {
            indexer_identifier: client.init_component(chunk_identifier.clone(), true)?,
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

    fn read_from_chunk(&self, dir: &Path) -> Result<Option<TiKvIndexer>> {
        let chunk_opts = ChunkOpts::from_dir(dir)?;
        let client = self.backend_builder.get_instance()?;
        if let Some(val) = client.get(chunk_opts.identifier.clone())? {
            let indexer_identifier = Vec::from(&val[..16]);
            return Ok(
                Some(
                    TiKvIndexer {
                        client,
                        chunk_identifier: chunk_opts.identifier,
                        indexer_identifier,
                    }
                )
            );
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::Result;
    use crate::common::test_utils::DummyTiKvBackend;
    use crate::indexer::{TiKvIndexer, Indexer};
    use crate::label::{Labels, Label};
    use crate::backend::tikv::TiKvRawBackend;
    use crate::indexer::sled_indexer::KvIndexerProcessor;

    #[test]
    fn test_create_index() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let indexer = TiKvIndexer {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            indexer_identifier: "indexer".to_string().into_bytes(),
        };

        let labels = Labels::from_vec(
            vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
                .into_iter()
                .map(|(key, value)| Label::from_key_value(key, value)).collect());
        let time_series_id = 1u64;
        indexer.create_index(labels.clone(), time_series_id)?;

        let mut indexer_id = indexer.indexer_identifier.to_vec();
        let mut key1 = KvIndexerProcessor::encode_time_series_id(time_series_id).into_bytes();
        indexer_id.append(key1.as_mut());
        let result1 = dummy_backend.get(indexer_id)?;
        assert!(result1.is_some());
        assert_eq!(KvIndexerProcessor::encode_labels(&labels, false).into_bytes(), result1.unwrap());

        indexer_id = indexer.indexer_identifier.to_vec();
        let mut key2 = KvIndexerProcessor::encode_labels(&labels, true).into_bytes();
        indexer_id.append(key2.as_mut());
        let result2 = dummy_backend.get(indexer_id)?;
        assert!(result2.is_some());
        assert_eq!(Vec::from(&time_series_id.to_be_bytes()[..]), result2.unwrap());

        indexer_id = indexer.indexer_identifier.to_vec();
        for label in labels.vec(){
            let mut key = indexer_id.clone();
            key.append(KvIndexerProcessor::encode_label(&label).into_bytes().as_mut());
            let result = dummy_backend.get(key)?;
            assert!(result.is_some());
            assert_eq!(Vec::from(&time_series_id.to_be_bytes()[..]), result.unwrap());
        }
        Ok(())
    }
}