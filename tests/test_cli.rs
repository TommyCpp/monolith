use tempfile::TempDir;
use std::process::Command;
use std::env::current_dir;
use assert_cmd::cargo::CommandCargoExt;
use assert_cmd::assert::OutputAssertExt;
use failure::_core::time::Duration;
use std::thread;
use monolith::utils::get_file_from_dir;
use monolith::{DB_METADATA_FILENAME, Result};

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
fn cli_path() -> Result<()>{
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("monolith-server").unwrap();
    let mut child = cmd.current_dir(&temp_dir)
        .args(&["--file_dir", &temp_dir.path().as_os_str().to_str().unwrap()])
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("Closed before kiled");

    assert!(get_file_from_dir(temp_dir.as_ref(), DB_METADATA_FILENAME)?.is_some());
    Ok(())
}
