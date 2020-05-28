use crate::Result;
use tikv_client::{RawClient, Config};
use serde::{Serialize, Deserialize};

use uuid::Uuid;
use std::path::Path;
use std::fs::read;
use crate::common::test_utils::DummyTiKvBackend;

pub trait TiKvRawBackend: Send + Sync {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// Store chunk and storage/indexer mapping information
    fn init_component(&self, chunk_identifier: Vec<u8>, is_indexer: bool) -> Result<Vec<u8>> {
        let value = self.get(chunk_identifier.clone())?.unwrap_or(
            {
                let mut id = Uuid::nil().as_bytes().to_vec();
                let mut nil = Uuid::nil().as_bytes().to_vec();
                id.append(&mut nil);
                id
            }
        );
        if is_indexer {
            let mut indexer_id = Uuid::new_v4().as_bytes().to_vec();
            indexer_id.append(value[..16].to_vec().as_mut());
            self.set(chunk_identifier.clone(), indexer_id.clone());
            Ok(indexer_id)
        } else {
            let mut storage_id = Uuid::new_v4().as_bytes().to_vec();
            let mut res = vec![];
            res.append(value[16..].to_vec().as_mut());
            res.append(storage_id.as_mut());
            self.set(chunk_identifier.clone(), res);
            Ok(storage_id)
        }
    }
}

/// Wrapper of TiKv Raw client
///
/// Since it's shared backend between different storage. We need to find some way to identify
/// storage and indexer. Backend will assign a identifier(u64) to component.
/// We will use the chunk identifier as the key. the component's identifier as value to store the relationship.
/// The lay out would be
///
/// {Chunk identifier}(8 bytes) -> {Indexer identifier}(16 bytes){Storage identifier}(16 bytes)
///
struct TiKvRawBackendImpl {
    client: RawClient,
}

impl TiKvRawBackendImpl {}

impl TiKvRawBackend for TiKvRawBackendImpl {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let res: tikv_client::Result<()> = futures::executor::block_on(
            self.client
                .put(key, value)
        );
        Ok(res?)
    }


    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let res: tikv_client::Result<Option<tikv_client::Value>> = futures::executor::block_on(
            self.client
                .get(key)
        );
        Ok(res?.map(|v| v.into()))
    }
}


// Return shared backend
#[derive(Clone)]
pub struct TiKvRawBackendSingleton {
    config: Config,
    // if true, then instead of connect to a tikv instance, we use a in memory hashmap
    dry_run: bool,
}

impl TiKvRawBackendSingleton {
    pub fn new(config: Config) -> Result<TiKvRawBackendSingleton> {
        Ok(
            TiKvRawBackendSingleton {
                config,
                dry_run: false,
            }
        )
    }

    pub fn from_config_file(filepath: &Path) -> Result<TiKvRawBackendSingleton> {
        let config_file = TiKvBackendConfigFile::from_file(filepath)?;
        if config_file.dry_run {
            Ok(
                TiKvRawBackendSingleton {
                    config: Config::new(Vec::<String>::new()),
                    dry_run: true,
                })
        } else {
            TiKvRawBackendSingleton::new(Config::new(config_file.pd_endpoints))
        }
    }

    pub fn get_instance(&self) -> Result<Box<dyn TiKvRawBackend>> {
        Ok(
            if self.dry_run {
                Box::new(DummyTiKvBackend::new())
            } else {
                Box::new(TiKvRawBackendImpl {
                    client: RawClient::new(self.config.clone())?
                })
            }
        )
    }
}

impl Default for TiKvRawBackendSingleton {
    fn default() -> Self {
        TiKvRawBackendSingleton {
            config: Config::new(Vec::<String>::new()),
            dry_run: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TiKvBackendConfigFile {
    #[serde(default)]
    pd_endpoints: Vec<String>,
    //yaml: pd_endpoints
    #[serde(default)]
    dry_run: bool, //yaml: dry_run
}

impl TiKvBackendConfigFile {
    pub fn from_file(filepath: &Path) -> Result<TiKvBackendConfigFile> {
        let content = read(filepath)?;
        let config_file: TiKvBackendConfigFile = serde_yaml::from_slice(content.as_slice())?;
        Ok(config_file)
    }
}

impl Default for TiKvBackendConfigFile {
    fn default() -> Self {
        TiKvBackendConfigFile {
            pd_endpoints: vec![],
            dry_run: true,
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::backend::tikv::TiKvBackendConfigFile;

    #[test]
    fn test_read_yaml_file() -> Result<()> {
        let config_file = TiKvBackendConfigFile {
            pd_endpoints: vec!["127.0.0.1".to_string()],
            dry_run: false
        };
        let res = serde_yaml::to_string(&config_file)?;
        Ok(())
    }
}
