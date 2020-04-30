use crate::{Builder, Result};
use tikv_client::{RawClient, Config};

/// Wrapper of TiKv Raw client
pub struct TiKvRawBackend {
    client: RawClient
}

pub struct TiKvRawBackendBuilder {
    config: Config
}

impl TiKvRawBackendBuilder {
    fn new(config: Config) -> Result<TiKvRawBackendBuilder> {
        Ok(
            TiKvRawBackendBuilder {
                config
            }
        )
    }
}

impl Builder<TiKvRawBackend> for TiKvRawBackendBuilder {
    fn build(&self, _: String) -> Result<TiKvRawBackend> {
        Ok(
            TiKvRawBackend {
                client: RawClient::new(self.config.clone())?
            }
        )
    }
}