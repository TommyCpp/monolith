use monolith::chunk::{Chunk, ChunkOpts};
use monolith::indexer::{Indexer, SledIndexer};
use monolith::label::{Label, Labels};
use monolith::storage::{SledStorage, Storage};
use monolith::test_utils::Ingester;
use monolith::time_point::TimePoint;
use monolith::time_series::TimeSeriesId;
use monolith::Result;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

use std::collections::BTreeSet;
use std::iter::FromIterator;
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
    )
    .data;
    for s in series {
        for time_point in s.time_points() {
            storage.write_time_point(s.id(), time_point.timestamp, time_point.value);
        }
        indexer.create_index(s.meta_data().clone(), s.id());
    }
    let mut ops = ChunkOpts::default();
    ops.start_time = Some(0u64);
    ops.end_time = Some(1000u64);
    let chunk = Chunk::new(storage.clone(), indexer.clone(), &ops);
    let res1 = chunk.query(
        Labels::from_vec(vec![Label::from_key_value("test1", "1")]),
        0,
        100,
    )?;
    assert_eq!(res1.len(), 2);
    assert_eq!(
        res1.iter()
            .map(|ts| ts.id())
            .collect::<Vec<TimeSeriesId>>()
            .to_vec(),
        vec![1, 3]
    );

    let res2 = chunk.query(
        Labels::from_vec(vec![
            Label::from_key_value("test2", "2"),
            Label::from_key_value("test1", "1"),
        ]),
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
    )
    .data;
    let mut ops = ChunkOpts::default();
    ops.start_time = Some(0u64);
    ops.end_time = Some(1000u64);
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

#[test]
//todo: add similar test in benchmark
fn test_ingest_data() -> Result<()> {
    let ingester = Ingester::new(Some(100), Some(50), Some(10), 170000);
    let mut data_chunks = ingester.data.chunks(ingester.data.len() / 2);
    let first = Vec::from(data_chunks.next().unwrap());
    let second = Vec::from(data_chunks.next().unwrap());
    let dir = TempDir::new()?;
    let storage = SledStorage::new(dir.path().join("storage").as_path())?;
    let indexer = SledIndexer::new(dir.path().join("indexer").as_path())?;
    let ops = ChunkOpts {
        start_time: Some(170000),
        end_time: Some(330000),
        identifier: vec![10, 10, 10],
    };
    let chunk = Chunk::new(storage, indexer, &ops);
    let arc = Arc::new(chunk);

    let chunk_ref = arc.clone();
    let handler1 = thread::spawn(move || {
        for ts in first {
            for tp in ts.time_points() {
                chunk_ref.insert(ts.meta_data().clone(), tp.clone());
            }
        }
    });

    let chunk_ref = arc.clone();
    let handler2 = thread::spawn(move || {
        for ts in second {
            for tp in ts.time_points() {
                chunk_ref.insert(ts.meta_data().clone(), tp.clone());
            }
        }
    });

    handler1.join();
    handler2.join();

    for ts in ingester.data {
        let series = arc.query(ts.meta_data().clone(), 170000, 330000)?;
        assert_eq!(series.len(), 1);
        let set: BTreeSet<TimePoint> = BTreeSet::from_iter(ts.time_points().into_iter().cloned());
        for tp in series[0].time_points() {
            assert!(set.get(tp).is_some())
        }
    }

    Ok(())
}
