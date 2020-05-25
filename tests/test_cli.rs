use tempfile::TempDir;
use std::process::Command;
use std::env::current_dir;
use assert_cmd::cargo::CommandCargoExt;
use assert_cmd::assert::OutputAssertExt;
use failure::_core::time::Duration;
use std::{thread, fs};
use monolith::utils::get_file_from_dir;
use monolith::{DB_METADATA_FILENAME, Result, SLED_BACKEND, TIKV_BACKEND};
use std::io::Read;
use std::path::Path;

#[test]
fn cli_no_args() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("monolith-server").unwrap();
    let mut child = cmd.current_dir(&temp_dir)
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("Closed before kiled")
}

#[test]
fn cli_path() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("monolith-server").unwrap();
    let mut child = cmd.current_dir(&temp_dir)
        .args(&["--file_dir", &temp_dir.path().as_os_str().to_str().unwrap()])
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("Closed before kiled");

    //generate metadata
    assert!(get_file_from_dir(temp_dir.as_ref(), DB_METADATA_FILENAME)?.is_some());
    let mut content = fs::read_to_string(temp_dir.as_ref().join(DB_METADATA_FILENAME))?;
    assert_db_metadata(temp_dir.path(),
                       |s| s.contains("SledIndexer") && s.contains("SledStorage"));
    Ok(())
}

#[test]
fn cli_set_tikv_indexer_dry_run() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("monolith-server").unwrap();
    let mut child = cmd.current_dir(&temp_dir)
        .args(&["--file_dir", &temp_dir.path().as_os_str().to_str().unwrap(), "--storage", SLED_BACKEND, "--indexer", TIKV_BACKEND])
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("Closed before kiled");

    // assert generate metadata
    assert!(get_file_from_dir(temp_dir.as_ref(), DB_METADATA_FILENAME)?.is_some());
    let mut content = fs::read_to_string(temp_dir.as_ref().join(DB_METADATA_FILENAME))?;
    assert_db_metadata(temp_dir.as_ref(),
                       |s| content.contains("TiKvIndexer"));
    Ok(())
}

#[test]
fn cli_set_tikv_indexer_invalid_config_file() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("monolith-server").unwrap();
    let mut child = cmd.current_dir(&temp_dir)
        .args(&["--file_dir", &temp_dir.path().as_os_str().to_str().unwrap(), "--storage", SLED_BACKEND, "--indexer", TIKV_BACKEND, "--tikv_config", "./tests/invalid_tikv_config.yaml"])
        .assert()
        .failure();

    Ok(())
}

fn assert_db_metadata(dir: &Path, f: impl Fn(String) -> bool) -> Result<()> {
    let content = fs::read_to_string(dir.join(DB_METADATA_FILENAME))?;
    assert!(f(content));
    Ok(())
}