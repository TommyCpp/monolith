use monolith::{Result, TiKvBackendConfigFile, TiKvRawBackendSingleton};
use std::path::PathBuf;
use std::time::Duration;
use tikv_client::Config;

#[test]
fn test_read_tikv_config_file() -> Result<()> {
    let path = PathBuf::from("./tests/tikv_config.yaml");
    let config = TiKvBackendConfigFile::from_file(path.as_path())?;
    Ok(())
}
