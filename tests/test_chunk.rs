use monolith::storage::{SledStorage, Storage};
use monolith::{Result, Chunk, Indexer};
use tempfile::TempDir;
use monolith::indexer::SledIndexer;
use monolith::time_series::{TimeSeries, TimeSeriesId};
use monolith::label::{Labels, Label};
use monolith::time_point::{Timestamp, Value, TimePoint};

#[test]
fn test_query() -> Result<()> {
    let index_tmp = TempDir::new().unwrap();
    let indexer = SledIndexer::new(index_tmp.path())?;
    let storage_tmp = TempDir::new().unwrap();
    let storage = SledStorage::new(storage_tmp.path())?;
    let series = time_series_generator(
        vec![1, 2, 3],
        vec![
            vec![("test1", "1"), ("test2", "2")],
            vec![("test2", "2"), ("test3", "3")],
            vec![("test3", "3"), ("test1", "1"), ("test2", "2")]
        ],
        vec![
            vec![(12, 12.9), (16, 13.5), (17, 46.4), (33, 45.5)],
            vec![(120, 12.9), (160, 13.5), (161, 15.4), (167, -43.3)],
            vec![(11, 12.9), (16, 13.5)],
        ],
    );
    for s in series {
        for time_point in s.time_points() {
            storage.write_time_point(s.id(), time_point.timestamp, time_point.value);
        }
        indexer.create_index(s.meta_data().clone(), s.id());
    }

    let chunk = Chunk::new(storage.clone(), indexer.clone());
    let res1 = chunk.query(
        Labels::from(vec![Label::from("test1", "1")]),
        0, 100)?;
    assert_eq!(res1.len(), 2);
    assert_eq!(res1.iter()
                   .map(|ts| ts.id())
                   .collect::<Vec<TimeSeriesId>>().to_vec(), vec![1, 3]);

    let res2 = chunk.query(
        Labels::from(vec![Label::from("test2", "2"), Label::from("test1", "1")]),
        0, 1000,
    )?;
    assert_eq!(res2.len(), 2);


    Ok(())
}

fn time_series_generator(ids: Vec<TimeSeriesId>, metadata: Vec<Vec<(&str, &str)>>, data: Vec<Vec<(Timestamp, Value)>>) -> Vec<TimeSeries> {
    assert_eq!(ids.len(), metadata.len());
    assert_eq!(data.len(), metadata.len());

    let mut res = Vec::new();
    for i in 0..ids.len() {
        let mut meta = Vec::new();
        for d in metadata.get(i).unwrap() {
            meta.push(Label::from(d.clone().0, d.clone().1));
        }
        let mut time_points = Vec::new();
        for t in data.get(i).unwrap() {
            time_points.push(TimePoint::new(t.clone().0, t.clone().1));
        }
        let time_series = TimeSeries::from(*(ids.get(i).unwrap()), Labels::from(meta), time_points);
        res.push(time_series);
    }

    res
}