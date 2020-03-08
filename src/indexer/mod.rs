use crate::common::label::Labels;
use crate::common::time_series::TimeSeriesId;
use crate::Result;

mod sled_indexer;

pub trait Indexer {
    fn get_series_id_by_labels(&self, labels: Labels) -> Result<Vec<TimeSeriesId>>;

    fn update_index(&self, labels: Labels, time_series_id: TimeSeriesId) -> Result<()>;
}