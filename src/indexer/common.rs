use crate::common::label::Labels;
use crate::common::ops::OrderIntersect;
use crate::common::time_series::TimeSeriesId;
use crate::{MonolithErr, Result};
use std::ops::Index;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::thread;

///
/// Indexer is in charge of query appropriate time series based on the labels.
///
pub trait Indexer {
    /// Get all time series and their meta data which contains all labels in __labels__
    fn get_series_with_label_matching(&self, labels: Labels) -> Result<Vec<(TimeSeriesId, Labels)>>;

    fn get_series_id_with_label_matching(&self, labels: Labels) -> Result<Vec<TimeSeriesId>>;

    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<TimeSeriesId>>;


    ///
    /// time_series_id must be single increasing.
    /// __Will not re-sort__ the time_series_id in values
    /// Will create three kind of mapping:
    /// 1. mapping from each label to time series id, used to search by label
    /// 2. mapping form label set to time series id, used to find target series id by complete label set
    /// 3. mapping from time series id to label set, used to get all meta data from time series id.
    fn create_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()>;
}

pub fn intersect_time_series_id_vec(mut ts: Vec<Vec<TimeSeriesId>>) -> Result<Vec<TimeSeriesId>> {
    type TimeSeriesIdMatrix = Vec<Vec<TimeSeriesId>>;
    if ts.len() == 0 {
        return Ok(Vec::new());
    }
    if ts.len() == 1{
        return Ok(ts.index(0).clone());
    }
    if ts.len() == 2 {
        return Ok(ts.index(0).order_intersect(ts.index(1)));
    }
    if ts.len() % 2 != 0 {
        let last = ts.pop().ok_or(MonolithErr::InternalErr(
            "Should at least have one element in input".to_string(),
        ))?;
        Ok(intersect_time_series_id_vec(ts)?.order_intersect(&last))
    } else {
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
            sender.send(intersect_time_series_id_vec(data));
        }

        thread::spawn(move || handler(_s1, data1.clone()));
        thread::spawn(move || handler(_s2, data2.clone()));

        let (mut _finish1, mut _finish2) = (false, false);

        fn gather_res(
            receiver: &Receiver<Result<Vec<TimeSeriesId>>>,
        ) -> Result<Option<Vec<TimeSeriesId>>> {
            match receiver.try_recv() {
                Ok(res) => Ok(Some(res?)),
                Err(TryRecvError::Empty) => Ok(None),
                _ => Err(MonolithErr::InternalErr(
                    "Cannot receive value from channel".to_string(),
                )),
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

        intersect_time_series_id_vec(next_vec)
    }
}

#[cfg(test)]
mod test {
    use crate::common::time_series::TimeSeriesId;
    use crate::indexer::common::intersect_time_series_id_vec;
    use crate::Result;

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

    fn _test_intersect_time_series_id_vec(len: usize) -> Result<()> {
        let input = generate_time_series_id_vec(len)?;
        let expect = (1u64..100 - (len - 1) as u64).collect::<Vec<u64>>();
        let res = intersect_time_series_id_vec(input)?;
        assert_eq!(res, expect);
        Ok(())
    }

    pub fn generate_time_series_id_vec(len: usize) -> Result<Vec<Vec<TimeSeriesId>>> {
        fn setup(len: usize) -> Vec<Vec<TimeSeriesId>> {
            let mut input = Vec::new();
            for i in 0..len {
                let v = (1u64..100 - i as u64).collect();
                input.push(v);
            }

            input
        }
        Ok(setup(len))
    }
}
