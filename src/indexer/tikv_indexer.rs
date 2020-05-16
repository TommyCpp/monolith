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
use std::sync::Arc;
use std::path::PathBuf;

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
                .filter(|(_id, res)|
                    res.is_ok())
                // Unwrap the valid one
                .map(|(id, res)|
                    (id, res.unwrap()))
                // Choose the one with value
                .filter(|(_id, option)| option.is_some())
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
                .filter(|(_id, res)| res.is_ok())
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
        let key = KvIndexerProcessor::encode_labels(&labels, true);
        Ok(
            self.client
                .get(self.add_indexer_id(key.into_bytes().as_mut()))?
                .and_then(
                    |v| Some(TimeSeriesId::from_be_bytes(v.as_slice().try_into().unwrap())))
        )
    }

    fn create_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()> {
        let _indexer_id_str: String = String::from_utf8_lossy(self.indexer_identifier.clone().as_slice()).into();

        {
            //Create Time series id to label map
            let key = KvIndexerProcessor::encode_time_series_id(time_series_id);
            self.client.set(self.add_indexer_id(key.into_bytes().as_mut()),
                            KvIndexerProcessor::encode_labels(&labels, false).into_bytes());
        }

        {
            //Create map between full label set to time series id
            let key = KvIndexerProcessor::encode_labels(&labels, true);
            self.client.set(self.add_indexer_id(key.into_bytes().as_mut()),
                            Vec::from(&time_series_id.to_be_bytes()[..]));
        }

        {
            // Create Reverse Search
            labels.vec().iter()
                .map(|label|
                    self.add_indexer_id(KvIndexerProcessor::encode_label(label).into_bytes().as_mut()))
                .map(|key| (key.clone(), self.client.get(key.to_vec())))
                .filter(|(_key, res)| res.is_ok())
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
    fn build(&self, path: String, chunk_opts: Option<&ChunkOpts>, _: Option<&DbOpts>) -> Result<TiKvIndexer> {
        std::fs::create_dir_all(PathBuf::from(path).join("indexer"))?;
        let client = self.backend_builder.get_instance()?;
        let chunk_identifier = chunk_opts.ok_or(MonolithErr::OptionErr)?.identifier.clone();
        let instance = TiKvIndexer {
            indexer_identifier: client.init_component(chunk_identifier.clone(), true)?,
            client,
            chunk_identifier,
        };
        Ok(
            instance
        )
    }

    fn write_to_chunk(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_from_chunk(&self, dir: &Path) -> Result<Option<TiKvIndexer>> {
        let chunk_opts = ChunkOpts::read_config_from_dir(dir)?;
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

    fn write_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn read_config(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    
    use crate::Result;
    use crate::common::test_utils::DummyTiKvBackend;
    use crate::indexer::{TiKvIndexer, Indexer};
    use crate::label::{Labels, Label};
    use crate::backend::tikv::TiKvRawBackend;
    use crate::indexer::sled_indexer::KvIndexerProcessor;
    
    

    /// Get test data for this unit test
    fn get_data(id: i32) -> Labels {
        if id == 0 {
            // first set of data
            Labels::from_vec(
                vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
                    .into_iter()
                    .map(|(key, value)| Label::from_key_value(key, value)).collect())
        } else {
            //second set of data
            Labels::from_vec(
                vec![("key1", "value1"), ("key2", "value1"), ("key3", "value3")]
                    .into_iter()
                    .map(|(key, value)| Label::from_key_value(key, value))
                    .collect()
            )
        }
    }

    #[test]
    fn test_create_index() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let indexer = TiKvIndexer {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            indexer_identifier: "indexer".to_string().into_bytes(),
        };

        let labels = get_data(0);
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
        for label in labels.vec() {
            let mut key = indexer_id.clone();
            key.append(KvIndexerProcessor::encode_label(&label).into_bytes().as_mut());
            let result = dummy_backend.get(key)?;
            assert!(result.is_some());
            assert_eq!(Vec::from(&time_series_id.to_be_bytes()[..]), result.unwrap());
        }
        Ok(())
    }

    #[test]
    fn test_get_series_id_by_labels() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let indexer = TiKvIndexer {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            indexer_identifier: "indexer".to_string().into_bytes(),
        };
        let labels = get_data(0);
        let time_series_id = 1u64;
        indexer.create_index(labels.clone(), time_series_id)?;

        let res = indexer.get_series_id_by_labels(labels)?;
        assert!(res.is_some());
        assert_eq!(res.unwrap(), 1);

        Ok(())
    }

    #[test]
    fn test_get_series_id_contains_labels() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let indexer = TiKvIndexer {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            indexer_identifier: "indexer".to_string().into_bytes(),
        };
        let labels_1 = get_data(0);
        let labels_2 = get_data(1);
        indexer.create_index(labels_1.clone(), 1u64)?;
        indexer.create_index(labels_2.clone(), 2u64)?;

        let label1 = Label::from_key_value("key1", "value1");
        let label2 = Label::from_key_value("key2", "value2");
        let res1 = indexer.get_series_id_contains_labels(Labels::from_vec(vec![label1]))?;
        assert_eq!(res1.len(), 2);
        assert!(res1.iter().position(|x| *x == 1).is_some());
        assert!(res1.iter().position(|x| *x == 2).is_some());

        let res2 = indexer.get_series_id_contains_labels(Labels::from_vec(vec![label2]))?;
        assert_eq!(res2.len(), 1);
        assert_eq!(res2, vec![1]);


        Ok(())
    }

    #[test]
    fn test_get_series_metadata_contains_labels() -> Result<()> {
        let dummy_backend = DummyTiKvBackend::new();
        let indexer = TiKvIndexer {
            client: Box::new(dummy_backend.clone()),
            chunk_identifier: "whatever".to_string().into_bytes(),
            indexer_identifier: "indexer".to_string().into_bytes(),
        };
        let labels_1 = get_data(0);
        let labels_2 = get_data(1);
        indexer.create_index(labels_1.clone(), 1u64)?;
        indexer.create_index(labels_2.clone(), 2u64)?;

        let label1 = Label::from_key_value("key1", "value1");
        let label2 = Label::from_key_value("key2", "value2");

        let res1 = indexer.get_series_metadata_contains_labels(Labels::from_vec(vec![label1]))?;
        assert_eq!(res1.len(), 2);
        assert!(res1.iter().position(|(id, _labels)| {
            *id == 1
        }).is_some());
        assert!(res1.iter().position(|(id, _labels)| {
            *id == 2
        }).is_some());

        let res2 = indexer.get_series_metadata_contains_labels(Labels::from_vec(vec![label2]))?;
        assert_eq!(res2.len(), 1);
        let (id, _labels) = res2[0].clone();
        assert_eq!(id, 1);


        Ok(())
    }
}