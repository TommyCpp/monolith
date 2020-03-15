use monolith::storage::{SledStorage, Storage};
use monolith::Result;
use tempfile::TempDir;

#[test]
fn test_set_time_point() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let sled_storage = SledStorage::new(temp_dir.path())?;
    let sled_ref = &sled_storage;
    sled_ref.write_time_point(1, 129 as u64, 11 as f64)?;
    sled_ref.write_time_point(1, 123 as u64, 160.2 as f64)?;

    let db = sled_storage.get_storage();
    match db.get("TS-1")? {
        Some(val) => assert_eq!(
            format!("{}", String::from_utf8(val.to_vec())?),
            "129,11/123,160.2"
        ),
        None => assert_eq!(1, 0), //fail
    }

    Ok(())
}

#[test]
fn test_get_time_series() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let sled_storage = SledStorage::new(temp_dir.path())?;
    let sled_ref = &sled_storage;
    for i in 123..444 {
        sled_ref.write_time_point(1, i as u64, 1253.0 as f64)?;
    }
    let series = sled_storage.read_time_series(1, 222, 333);
    assert!(series.is_ok());
    let timepoint_vec = series.unwrap();
    assert_eq!(timepoint_vec.first().unwrap().timestamp, 222);
    assert_eq!(timepoint_vec.last().unwrap().timestamp, 333);
    Ok(())
}
