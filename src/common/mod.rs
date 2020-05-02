use crate::common::ops::OrderIntersect;
use crate::Result;
use crate::common::time_series::TimeSeriesId;
use std::ops::Index;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use crate::chunk::ChunkOpts;
use crate::common::option::DbOpts;


pub mod label;
pub mod option;
pub mod time_point;
pub mod time_series;
pub mod utils;
pub mod metadata;

pub mod test_utils;

pub mod ops {
    pub trait OrderIntersect {
        fn order_intersect(&self, other: &Self) -> Self;
    }
}

impl OrderIntersect for Vec<TimeSeriesId> {
    fn order_intersect(&self, other: &Self) -> Self {
        let mut res: Vec<TimeSeriesId> = Vec::new();
        let mut i = 0;
        let mut j = 0;
        while i < self.len() && j < other.len() {
            if self.index(i) == other.index(j) {
                res.push(*self.index(i));
                i += 1;
                j += 1;
            } else if self.index(i) > other.index(j) {
                j += 1;
            } else {
                i += 1;
            }
        }
        res
    }
}

pub struct IdGenerator(AtomicU64);

impl IdGenerator {
    pub fn new(init_id: TimeSeriesId) -> IdGenerator {
        IdGenerator(AtomicU64::new(init_id))
    }
    pub fn next(&self) -> TimeSeriesId {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

/// Build a Indexer or Storage object.
///
/// Implementation may add more function to let user pass more configs or options
pub trait Builder<T> {
    fn build(&self, path: String, chunk_opts: Option<&ChunkOpts>, db_opts: Option<&DbOpts>) -> Result<T>;
}

pub trait HasTypeName {
    fn get_type_name() -> &'static str;
}


#[cfg(test)]
mod test {
    use crate::common::ops::OrderIntersect;
    use crate::common::time_series::TimeSeriesId;
    use crate::common::IdGenerator;
    use std::ops::Index;

    #[test]
    fn generate_id() {
        let id_generator = IdGenerator::new(2);
        assert_eq!(id_generator.next(), 2);
        assert_eq!(id_generator.next(), 3)
    }

    #[test]
    fn order_intersect() {
        let vec1: Vec<TimeSeriesId> = vec![1, 2, 3, 4, 5];
        let vec2: Vec<TimeSeriesId> = vec![1, 2, 3, 4, 5, 6, 7];
        let res = vec1.order_intersect(&vec2);
        for i in 1..5 {
            assert_eq!(i as u64, *res.index(i - 1))
        }
    }
}
