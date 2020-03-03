use crate::common::time_series::TimeSeriesId;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicU64;

pub(crate) mod label;
pub(crate) mod time_point;
pub(crate) mod time_series;
pub mod option;

pub struct IdGenerator(AtomicU64);

impl IdGenerator {
    pub fn new(init_id: TimeSeriesId) -> IdGenerator {
        IdGenerator(AtomicU64::new(init_id))
    }
    pub fn next(&self) -> TimeSeriesId {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod test {
    use crate::common::IdGenerator;

    #[test]
    fn generate_id() {
        let id_generator = IdGenerator::new(2);
        assert_eq!(id_generator.next(), 2);
        assert_eq!(id_generator.next(), 3)
    }
}