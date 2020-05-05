use crate::{Builder, Result, IdGenerator};
use tikv_client::{RawClient, Config};
use futures::prelude::*;
use tikv_client::raw::Client;
use uuid::Uuid;

pub trait TiKvRawBackend {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// Store chunk and storage/indexer mapping information
    fn init_component(&self, chunk_identifier: Vec<u8>, is_indexer: bool) -> Result<Vec<u8>> {
        let mut value = self.get(chunk_identifier.clone())?.unwrap_or(
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
/// ```not rust
/// {Chunk identifier}(8 bytes) -> {Indexer identifier}(16 bytes){Storage identifier}(16 bytes)
/// ```
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
pub struct TiKvRawBackendSingleton {
    config: Config
}

impl TiKvRawBackendSingleton {
    pub fn new(config: Config) -> Result<TiKvRawBackendSingleton> {
        Ok(
            TiKvRawBackendSingleton {
                config
            }
        )
    }
    pub fn get_instance(&self) -> Result<Box<dyn TiKvRawBackend>> {
        Ok(
            Box::new(TiKvRawBackendImpl {
                client: RawClient::new(self.config.clone())?
            })
        )
    }
}
