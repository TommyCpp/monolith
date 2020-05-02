use crate::{Builder, Result, IdGenerator};
use tikv_client::{RawClient, Config};
use futures::prelude::*;
use tikv_client::raw::Client;

pub trait TiKvRawBackend {
    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;
}

/// Wrapper of TiKv Raw client
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
pub struct TakeBackendSingleton {
    config: Config
}

impl TakeBackendSingleton {
    pub fn new(config: Config) -> Result<TakeBackendSingleton> {
        Ok(
            TakeBackendSingleton {
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
