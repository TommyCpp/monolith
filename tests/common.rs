use std::collections::BTreeMap;

use crate::common::label::Labels;
use crate::common::time_series::{IdGenerator, TimeSeries};
use crate::label::{Label, Labels};

#[test]
fn crate_time_series() {
    let time_series = TimeSeries::new(12, Labels::new());
}

#[test]
fn generate_id() {
    let id_generator = IdGenerator::new(2);
    assert_eq!(id_generator.next(), 2);
    assert_eq!(id_generator.next(), 3)
}

#[test]
fn create_timepoint() {
    let timepoint = TimePoint::new(120, 12.0);
    assert_eq!(timepoint.timestamp, 120);
    assert_eq!(timepoint.value, 12.0);
}


#[test]
fn get_hash() {
    let mut labels = Labels::new();
    labels.add(Label::from("test", "test"));
    print!("{}", labels.get_hash())
}

#[test]
fn test_new_database() {
    let chunk = Chunk::new();
    assert_eq!(chunk.end_time - chunk.start_time, CHUCK_SIZE)
}

#[test]
fn test_create_time_series() {
    let mut db = Chunk::new();
    let mut labels: Labels = Labels::new();
    labels.add(Label::new(String::from("test"), String::from("series")));
    db.create_series(labels, 12);
    assert_eq!(db.label_series.len(), 1)
}

#[test]
fn test_get_series_id_by_label() {
    //create timeseries
    let mut db = Chunk::new();
    let mut labels: Labels = Labels::new();
    labels.add(Label::new(String::from("test"), String::from("series")));
    db.create_series(labels.clone(), 12);

    //get series id by labels
    let query: Label = Label::new(String::from("test"), String::from("series"));
    {
        let res = db.get_series_id_by_label(&query).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(*res.get(0).unwrap(), 12 as u64);
    }
    {
        db.create_series(labels.clone(), 11);
        let res = db.get_series_id_by_label(&query).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(*res.get(1).unwrap(), 11 as u64);
    }
}

#[test]
fn test_get_series_id_by_labels() {
    let mut db = Chunk::new();
    let mut i = 0;
    let mut labels_ts_1 = Labels::new();
    labels_ts_1.add(Label::new(String::from("test1"), String::from("value1")));
    labels_ts_1.add(Label::new(String::from("test2"), String::from("value2")));
    labels_ts_1.add(Label::new(String::from("test3"), String::from("value2")));
    labels_ts_1.add(Label::new(String::from("test4"), String::from("value2")));

    let mut labels_ts_2 = Labels::new();
    labels_ts_2.add(Label::new(String::from("test1"), String::from("value1")));
    labels_ts_2.add(Label::new(String::from("test2"), String::from("value2")));
    labels_ts_2.add(Label::new(String::from("test3"), String::from("value2")));

    db.create_series(labels_ts_1.clone(), 11);
    db.create_series(labels_ts_2.clone(), 12);

    let label1 = Label::new(String::from("test1"), String::from("value1"));
    let label2 = Label::new(String::from("test2"), String::from("value2"));
    let label3 = Label::new(String::from("test3"), String::from("value2"));


    let mut target = vec![label1, label2, label3];
    match db.get_series_to_insert(&target) {
        Some(res) => {
            assert_eq!(res, 12)
        }
        None => {
            assert_eq!(true, false) //fail the test
        }
    }
}

#[test]
fn test_timestamp_in_range() {
    let mut db = Chunk::new();
    db.start_time = 10000 as Timestamp;
    db.end_time = 15000 as Timestamp;
    let ts1 = 12000 as Timestamp;
    let ts2 = 1000 as Timestamp;
    assert_eq!(db.is_in_range(&ts1), true);
    assert_eq!(db.is_in_range(&ts2), false);
}

#[test]
fn test_insert() {
    let mut db = Chunk::new();
    db.start_time = 10000 as Timestamp;
    db.end_time = 15000 as Timestamp;
    let mut meta_data = Labels::new();
    let label_x = Label::from("x", "y");
    meta_data.add(label_x);
    db.insert(11000 as Timestamp, 10 as f64, meta_data);
    let res = db.get_series_id_by_label(&Label::from("x", "y")).unwrap();
    assert_eq!(1, res.len());
    assert_eq!(res.get(0), Some(&0));
}