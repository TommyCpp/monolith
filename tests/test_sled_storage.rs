use monolith::storage::{SledStorage, Storage};
use monolith::{Result, Timestamp, Value};
use tempfile::TempDir;
use monolith::test_utils::Ingester;



#[test]
fn test_set_time_point() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let sled_storage = SledStorage::new(temp_dir.path())?;
    let sled_ref = &sled_storage;
    sled_ref.write_time_point(1, 123 as u64, 160.2 as f64)?;

    let db = sled_storage.get_storage();
    match db.get("TS1")? {
        Some(val) => {
            let val_vec = AsRef::<[u8]>::as_ref(&val).to_vec();
            assert_eq!(val_vec.len(), std::mem::size_of::<Timestamp>() + std::mem::size_of::<Value>());
        }
        None => assert_eq!(1, 0), //fail
    }

    Ok(())
}

#[test]
fn test_get_time_series() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = SledStorage::new(temp_dir.path())?;

    let ingester = Ingester::new(None, None, None, 10);
    for (idx, series) in ingester.data.iter().enumerate() {
        for tp in series.time_points() {
            storage.write_time_point(idx as u64, tp.timestamp, tp.value);
        }
    }

    for (idx, series) in ingester.data.iter().enumerate() {
        let res_series = storage.read_time_series(idx as u64, 0, 1000000)?;
        let expect_series = series.time_points();
        for idx in 0..expect_series.len() {
            assert_eq!(res_series[idx], expect_series[idx])
        }
    }

    Ok(())
}
