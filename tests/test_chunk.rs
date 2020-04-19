use monolith::indexer::{SledIndexer, Indexer};
use monolith::label::{Label, Labels};
use monolith::storage::{SledStorage, Storage};
use monolith::time_point::{TimePoint, Timestamp, Value};
use monolith::time_series::{TimeSeries, TimeSeriesId};
use monolith::Result;
use tempfile::TempDir;
use monolith::chunk::{ChunkOpts, Chunk};
use monolith::test_utils::Ingester;

#[test]
fn test_query() -> Result<()> {
    let index_tmp = TempDir::new().unwrap();
    let indexer = SledIndexer::new(index_tmp.path())?;
    let storage_tmp = TempDir::new().unwrap();
    let storage = SledStorage::new(storage_tmp.path())?;
    let series = Ingester::from_data(
        vec![1, 2, 3],
        vec![
            vec![("test1", "1"), ("test2", "2")],
            vec![("test2", "2"), ("test3", "3")],
            vec![("test3", "3"), ("test1", "1"), ("test2", "2")],
        ],
        vec![
            vec![(12, 12.9), (16, 13.5), (17, 46.4), (33, 45.5)],
            vec![(120, 12.9), (160, 13.5), (161, 15.4), (167, -43.3)],
            vec![(11, 12.9), (16, 13.5)],
        ],
    ).data;
    for s in series {
        for time_point in s.time_points() {
            storage.write_time_point(s.id(), time_point.timestamp, time_point.value);
        }
        indexer.create_index(s.meta_data().clone(), s.id());
    }
    let ops = ChunkOpts {
        start_time: Some(0u64),
        end_time: Some(1000u64),
    };
    let chunk = Chunk::new(storage.clone(), indexer.clone(), &ops);
    let res1 = chunk.query(Labels::from_vec(vec![Label::from_key_value("test1", "1")]), 0, 100)?;
    assert_eq!(res1.len(), 2);
    assert_eq!(
        res1.iter()
            .map(|ts| ts.id())
            .collect::<Vec<TimeSeriesId>>()
            .to_vec(),
        vec![1, 3]
    );

    let res2 = chunk.query(
        Labels::from_vec(vec![Label::from_key_value("test2", "2"), Label::from_key_value("test1", "1")]),
        0,
        1000,
    )?;
    assert_eq!(res2.len(), 2);

    Ok(())
}

#[test]
fn test_insert() -> Result<()> {
    let index_tmp = TempDir::new().unwrap();
    let indexer = SledIndexer::new(index_tmp.path())?;
    let storage_tmp = TempDir::new().unwrap();
    let storage = SledStorage::new(storage_tmp.path())?;
    let series = Ingester::from_data(
        vec![1, 2, 3],
        vec![
            vec![("test1", "1"), ("test2", "2")],
            vec![("test2", "2"), ("test3", "3")],
            vec![("test1", "1"), ("test2", "2"), ("test3", "3")],
        ],
        vec![
            vec![(12, 12.9), (16, 13.5), (17, 46.4), (33, 45.5)],
            vec![(120, 12.9), (160, 13.5), (161, 15.4), (167, -43.3)],
            vec![(11, 12.9), (16, 13.5)],
        ],
    ).data;
    let ops = ChunkOpts {
        start_time: Some(0u64),
        end_time: Some(1000u64),
    };
    let chunk = Chunk::new(storage.clone(), indexer.clone(), &ops);

    for s in series.clone() {
        for tp in s.time_points() {
            chunk.insert(s.meta_data().clone(), tp.clone())?;
        }
    }

    for s in series {
        let idx = indexer.get_series_id_by_labels(s.meta_data().clone())?;
        assert_eq!(idx.is_some(), true);
        let tps = storage.read_time_series(idx.unwrap(), 0u64, 1000u64)?;
        assert_eq!(tps.len(), s.time_points().len());
        assert_eq!(tps, s.time_points().clone())
    }

    Ok(())
}

