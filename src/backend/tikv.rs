use crate::{Builder, Result};
use tikv_client::{RawClient, Config};
use futures::prelude::*;
use tikv_client::raw::Client;

pub trait TiKvRawBackend {
    fn set(&self, key: Vec<u8>, value: Vec<u8>, cf: String) -> Result<()>;
    fn get(&self, key: Vec<u8>, cf: String) -> Result<Option<Vec<u8>>>;
}

/// Wrapper of TiKv Raw client
struct TiKvRawBackendImpl {
    client: RawClient,
}

impl TiKvRawBackendImpl {
    fn with_cf(&self, cf: String) -> Client{
        self.client
            .with_cf(tikv_client::ColumnFamily::from(cf))
    }
}

impl TiKvRawBackend for TiKvRawBackendImpl {
    fn set(&self, key: Vec<u8>, value: Vec<u8>, cf: String) -> Result<()> {
        let res: tikv_client::Result<()> = futures::executor::block_on(
            self.with_cf(cf)
                .put(key, value)
        );
        Ok(res?)
    }


    fn get(&self, key: Vec<u8>, cf: String) -> Result<Option<Vec<u8>>> {
        let res: tikv_client::Result<Option<tikv_client::Value>> = futures::executor::block_on(
            self.with_cf(cf)
                .get(key)
        );
        Ok(res?.map(|v| v.into()))
    }
}

pub struct TiKvRawBackendBuilder {
    config: Config
}

impl TiKvRawBackendBuilder {
    pub fn new(config: Config) -> Result<TiKvRawBackendBuilder> {
        Ok(
            TiKvRawBackendBuilder {
                config
            }
        )
    }
}

impl Builder<Box<dyn TiKvRawBackend>> for TiKvRawBackendBuilder {
    fn build(&self, _: String) -> Result<Box<dyn TiKvRawBackend>> {
        Ok(
            Box::new(TiKvRawBackendImpl {
                client: RawClient::new(self.config.clone())?
            })
        )
    }
}