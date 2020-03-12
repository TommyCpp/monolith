use sled::Db;
use std::sync::atomic::AtomicI32;
use std::sync::mpsc::*;
use crate::common::ops::OrderIntersect;
use crate::indexer::Indexer;
use crate::common::label::{Labels, Label};
use crate::MonolithErr;
use crate::Result;
use crate::common::time_series::TimeSeriesId;
use std::ops::{Add, Index, Deref};
use std::path::Path;
use std::thread;

const LABEL_REVERSE_PREFIX: &str = "LR";

///
/// Sled based indexer, use to search timeseries id based on metadata.
pub struct SledIndexer {
    storage: Db
}

impl SledIndexer {
    fn new(dir: &Path) -> Result<SledIndexer> {
        Ok(
            SledIndexer {
                storage: Db::start_default(dir)?
            }
        )
    }

    fn encode_label(label: &Label) -> String {
        format!("{}={}", label.key(), label.value())
    }

    fn encode_labels(labels: &Labels) -> String {
        let mut time_series_meta = labels.clone();
        time_series_meta.sort();
        let mut res = String::new();
        for label in time_series_meta.vec() {
            res = res.add(format!("{}={},", label.key(), label.value()).as_str());
        }
        res.pop(); //remove last ,
        res
    }

    pub fn get(&self, label: &Label) -> Result<Option<Vec<TimeSeriesId>>> {
        let key = SledIndexer::encode_label(label);
        match self.storage.get(&key)? {
            Some(val) => {
                let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
                let id_str: Vec<&str> = val_str.split(",").collect();
                let mut res = Vec::new();
                for id in id_str {
                    res.push(id.parse::<u64>()?);
                }
                Ok(Some(res))
            }
            None => Ok(None)
        }
    }

    fn intersect_time_series_id_vec(mut ts: Vec<Vec<TimeSeriesId>>) -> Result<Vec<TimeSeriesId>> {
        type TimeSeriesIdMatrix = Vec<Vec<TimeSeriesId>>;
        if ts.len() == 0 {
            return Ok(Vec::new());
        }
        if ts.len() == 2 {
            return Ok(ts.index(0).order_intersect(ts.index(1)));
        }
        if ts.len() % 2 != 0 {
            let last = ts.pop().ok_or(MonolithErr::InternalErr("Should at least have one element in input".to_string()))?;
            Ok(SledIndexer::intersect_time_series_id_vec(ts)?.order_intersect(&last))
        } else {
            //todo: benchmark it
            let (_s1, _r1) = channel::<Result<Vec<TimeSeriesId>>>();
            let (_s2, _r2) = channel::<Result<Vec<TimeSeriesId>>>();
            let (mut data1, mut data2) = (Vec::new(), Vec::new());
            for i in 0..ts.len() {
                let _v: Vec<u64> = ts.index(i).clone();
                if i < ts.len() / 2 {
                    data1.push(_v)
                } else {
                    data2.push(_v)
                }
            }

            fn handler(sender: Sender<Result<Vec<TimeSeriesId>>>, data: TimeSeriesIdMatrix) {
                sender.send(SledIndexer::intersect_time_series_id_vec(data));
            }

            thread::spawn(move || { handler(_s1, data1.clone()) });
            thread::spawn(move || { handler(_s2, data2.clone()) });

            let (mut _finish1, mut _finish2) = (false, false);

            fn gather_res(receiver: &Receiver<Result<Vec<TimeSeriesId>>>) -> Result<Option<Vec<TimeSeriesId>>> {
                match receiver.try_recv() {
                    Ok(res) => {
                        Ok(Some(res?))
                    }
                    Err(TryRecvError::Empty) => {
                        Ok(None)
                    }
                    _ => {
                        Err(MonolithErr::InternalErr("Cannot receive value from channel".to_string()))
                    }
                }
            }

            let mut next_vec: TimeSeriesIdMatrix = Vec::new();


            loop {
                if !_finish1 {
                    if let Some(val) = gather_res(&_r1)? {
                        next_vec.push(val);
                        _finish1 = true
                    }
                }
                if !_finish2 {
                    if let Some(val) = gather_res(&_r2)? {
                        next_vec.push(val);
                        _finish2 = true
                    }
                }
                if _finish1 && _finish2 {
                    break;
                }
            }

            SledIndexer::intersect_time_series_id_vec(next_vec)
        }
    }
}

impl Indexer for SledIndexer {
    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>> {
        let mut ts_vec = Vec::new();
        for label in labels.vec() {
            if let Some(ts) = self.get(label)? {
                ts_vec.push(ts as Vec<TimeSeriesId>);
            }
        }

        SledIndexer::intersect_time_series_id_vec(ts_vec)
    }

    ///
    /// time_series_id must be single increasing.
    /// update_index will not re-sort the time_series_id in values
    fn update_index(&self, labels: Labels, time_series_id: u64) -> Result<()> {
        let tree = &self.storage;
        let keys: Vec<String> = labels.vec().iter()
            .map(SledIndexer::encode_label)
            .collect();
        for key in keys {
            let val = match tree.get(&key)? {
                None => format!("{}", time_series_id),
                Some(val) => {
                    let val_str = String::from_utf8(AsRef::<[u8]>::as_ref(&val).to_vec())?;
                    format!("{},{}", val_str, time_series_id)
                }
            };
            tree.set(&key, val.into_bytes());
            tree.flush();
        }

        Ok(())
    }
}

mod test {
    use crate::Result;
    use tempfile::TempDir;
    use crate::indexer::sled_indexer::SledIndexer;
    use crate::indexer::Indexer;
    use crate::common::label::{Labels, Label};
    use crate::common::time_series::TimeSeriesId;
    use std::env::var;

    #[test]
    fn test_update_index() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let indexer = SledIndexer::new(temp_dir.path())?;
        let mut labels = Labels::new();
        labels.add(Label::from("test1", "test1value"));
        labels.add(Label::from("test2", "test1value"));
        labels.add(Label::from("test3", "test1value"));
        indexer.update_index(labels, 1)?;

        let label1 = indexer.storage.get("test1=test1value")?.unwrap();
        let label2 = indexer.storage.get("test2=test1value")?.unwrap();
        let val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&label1).to_vec())?;
        let val_str_2 = String::from_utf8(AsRef::<[u8]>::as_ref(&label2).to_vec())?;
        assert_eq!(val_str_1, val_str_2);

        let mut another_labels = Labels::new();
        another_labels.add(Label::from("test1", "test1value"));
        indexer.update_index(another_labels, 2);
        let another_label1 = indexer.storage.get("test1=test1value")?.unwrap();
        let another_val_str_1 = String::from_utf8(AsRef::<[u8]>::as_ref(&another_label1).to_vec())?;
        assert_eq!("1,2", another_val_str_1);


        Ok(())
    }

    #[test]
    fn test_intersect_time_series_id_vec() -> Result<()> {
        //test when there are exactly two
        _test_intersect_time_series_id_vec(2);
        _test_intersect_time_series_id_vec(3);
        _test_intersect_time_series_id_vec(4);
        _test_intersect_time_series_id_vec(60);
        _test_intersect_time_series_id_vec(71);

        Ok(())
    }

    fn _test_intersect_time_series_id_vec(len: usize) -> Result<()>{
        fn setup(len: usize) -> Vec<Vec<TimeSeriesId>> {
            let mut input = Vec::new();
            for i in 0..len {
                let v = (1u64..100 - i as u64).collect();
                input.push(v);
            }

            input
        }
        let input = setup(len);
        let expect = (1u64..100 - (len - 1) as u64).collect::<Vec<u64>>();
        let res = SledIndexer::intersect_time_series_id_vec(input)?;
        assert_eq!(res, expect);
        Ok(())
    }

}