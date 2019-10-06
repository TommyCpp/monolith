pub mod time_point;
pub mod chunk;
pub mod time_series;
pub mod label;

use time_point::Timestamp;
use crate::time_point::TimePoint;


fn main() {
    let t: TimePoint = TimePoint::new(12, 12.0);
}
