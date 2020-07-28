use crate::common::label::Labels;

use crate::common::time_series::TimeSeriesId;
use crate::{HasTypeName, Result};

///
/// Indexer is in charge of query appropriate time series based on the labels.
///
pub trait Indexer: Sized + HasTypeName {
    /// Get all time series, and their metadata which contains __all__ labels
    ///
    /// Note that the result time series may contains other labels
    fn get_series_metadata_contains_labels(
        &self,
        labels: Labels,
    ) -> Result<Vec<(TimeSeriesId, Labels)>>;

    /// Get all time series that contains __all__ label in labels
    ///
    /// Note that the result time series may contains other labels
    fn get_series_id_contains_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>>;

    /// Get time series that match exactly with the labels
    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<TimeSeriesId>>;

    ///
    /// time_series_id must be single increasing.
    /// __Will not re-sort__ the time_series_id in values
    /// Will create three kinds of mapping:
    /// 1. mapping from each label to time series id, used to search by label
    /// 2. mapping form label set to time series id, used to find target series id by complete label set
    /// 3. mapping from time series id to label set, used to get all meta data from time series id.
    fn create_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()>;
}

#[cfg(test)]
mod test {
    use crate::common::time_series::TimeSeriesId;
    use crate::utils::intersect_time_series_id_vec;
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
