use crate::{Builder, Result, TiKvRawBackendSingleton, HasTypeName, MonolithErr};
use std::path::Path;
use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;
use crate::indexer::Indexer;
use crate::common::label::Labels;
use crate::backend::tikv::TiKvRawBackend;
use crate::indexer::sled_indexer::KvIndexerProcessor;
use crate::common::time_series::TimeSeriesId;

const LABEL_REVERSE_PREFIX: &str = "LR";
const LABEL_PREFIX: &str = "L";
const ID_PREFIX: &str = "I";

pub struct TiKvIndexer {
    client: Box<dyn TiKvRawBackend>,
    chunk_identifier: Vec<u8>,
    indexer_identifier: Vec<u8>,
}

impl TiKvIndexer {
}

impl Indexer for TiKvIndexer {
    fn get_series_with_label_matching(&self, labels: Labels) -> Result<Vec<(u64, Labels)>> {
        unimplemented!()
    }

    fn get_series_id_with_label_matching(&self, labels: Labels) -> Result<Vec<u64>> {
        unimplemented!()
    }

    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<u64>> {
        unimplemented!()
    }

    fn create_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()> {
        let indexer_id_str: String = String::from_utf8_lossy(self.indexer_identifier.clone().as_slice()).into();

        {
            //Create Time series id to label map
            let key = KvIndexerProcessor::encode_time_series_id(time_series_id);
            self.client.set(key.into_bytes(),
                            KvIndexerProcessor::encode_labels(&labels, false).into_bytes());
        }

        {
            //Create map between full label set to time series id
            let key = KvIndexerProcessor::encode_labels(&labels, true);
            self.client.set(key.into_bytes(), Vec::from(&time_series_id.to_be_bytes()[..]));
        }

        {
            // Create Reverse Search
            labels.vec().iter()
                .map(|label|
                    format!("{}{}", indexer_id_str.clone(), KvIndexerProcessor::encode_label(label).as_str()))
                .map(|key| (key.clone(), self.client.get(key.as_bytes().to_vec())))
                .filter(|(key, res)| res.is_ok())
                .map(|(key, res)| {
                    match res.unwrap() {
                        Some(mut val) => {
                            val.append(Vec::from(&time_series_id.to_be_bytes()[..]).as_mut());
                            self.client.set(key.clone().into_bytes(), val);
                        }
                        None => {
                            self.client.set(key.clone().into_bytes(),
                                            Vec::from(&time_series_id.to_be_bytes()[..]));
                        }
                    }
                });
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