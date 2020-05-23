use monolith::{Result, TiKvRawBackendSingleton, TiKvBackendConfigFile};
use std::path::PathBuf;
use tikv_client::Config;
use std::time::Duration;

#[test]
fn test_read_tikv_config_file() -> Result<()>{
    let path = PathBuf::from("./tests/tikv_config.yaml");
    let config = TiKvBackendConfigFile::from_file(path.as_path())?;
    Ok(())
}