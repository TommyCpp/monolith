use crate::ops::OrderIntersect;
use crate::time_series::TimeSeriesId;
use crate::{MonolithErr, Result, Timestamp};
use std::fs::File;
use std::ops::Index;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, thread};

pub fn is_duration_overlap(
    start_time_1: Timestamp,
    end_time_1: Timestamp,
    start_time_2: Timestamp,
    end_time_2: Timestamp,
) -> bool {
    return !(start_time_1 > end_time_2 || end_time_1 < start_time_2);
}

pub fn get_current_timestamp() -> Timestamp {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_the_epoch.as_millis() as Timestamp
}

pub fn encode_chunk_dir(start_time: Timestamp, end_time: Timestamp) -> String {
    let mut res = 0u128;
    res |= start_time as u128;
    res = res << 64;
    res |= end_time as u128;

    format!("{:x}", res)
}

pub fn decode_chunk_dir(dir_name: String) -> Result<(Timestamp, Timestamp)> {
    let decimal = u128::from_str_radix(dir_name.as_str(), 16)?;
    let end_time: u64 = (decimal & (u64::max_value() as u128)) as u64;
    let start_time: u64 = ((decimal & (u128::max_value() - u64::max_value() as u128)) >> 64) as u64;

    Ok((start_time, end_time))
}

/// Given a list of `TimeSeriesId` array, this function return the the `TimeSeriesId` that occur in all array
///
/// Note that the element in each `TimeSeriesId` array must be in __ascend order__.
///
/// # Examples
/// ```
/// use monolith::utils::intersect_time_series_id_vec;
/// let v1 = vec![1u64, 2u64, 3u64, 4u64];
/// let v2 = vec![1u64, 4u64];
/// let mut ts = vec![v1, v2];
///
/// let res = intersect_time_series_id_vec(ts).unwrap();
/// ```
pub fn intersect_time_series_id_vec(mut ts: Vec<Vec<TimeSeriesId>>) -> Result<Vec<TimeSeriesId>> {
    type TimeSeriesIdMatrix = Vec<Vec<TimeSeriesId>>;
    if ts.len() == 0 {
        return Ok(Vec::new());
    }
    if ts.len() == 1 {
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

/// Read file from dir, filename must be constant
///
/// Return None if no such file found
pub fn get_file_from_dir(base_dir: &Path, filename: &'static str) -> Result<Option<File>> {
    let file = Option::transpose(fs::read_dir(base_dir)?.find(|entry| {
        entry.is_ok() && entry.as_ref().unwrap().file_name().into_string().unwrap() == filename
    }))
    .unwrap();

    return if file.is_some() {
        Ok(Some(File::open(file.unwrap().path())?))
    } else {
        Ok(None)
    };
}

#[cfg(test)]
mod tests {
    use crate::common::utils::{decode_chunk_dir, encode_chunk_dir, get_current_timestamp};
    use crate::Result;

    #[test]
    fn test_encode_chunk_dir() -> Result<()> {
        let start_time = 1671234234u64;
        let end_time = 14423141234u64;

        let res = encode_chunk_dir(start_time, end_time);
        assert_eq!(res, "639d02ba000000035bafab72");
        Ok(())
    }

    #[test]
    fn test_decode_chunk_dir() -> Result<()> {
        let dir_name = "639d02ba000000035bafab72";

        let (start_time, end_time) = decode_chunk_dir(String::from(dir_name))?;

        assert_eq!(start_time, 1671234234u64);
        assert_eq!(end_time, 14423141234u64);

        Ok(())
    }

    #[test]
    /// Make sure we get a valid timestamp that larger than 0
    fn test_get_current_timestamp() -> Result<()> {
        let current_timestamp = get_current_timestamp();
        if !(current_timestamp > 0) {
            assert_eq!(1, 0);
        }
        Ok(())
    }
}
