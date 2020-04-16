use crate::common::label::{Labels, Label};

use crate::common::time_point::{TimePoint, Timestamp, Value};
use crate::proto::Sample;



pub type TimeSeriesId = u64;
pub type LabelPointPairs = Vec<(Labels, Vec<TimePoint>)>;

#[derive(Clone)]
pub struct TimeSeries {
    id: TimeSeriesId,
    time_points: Vec<TimePoint>,
    meta_data: Labels,
}

impl TimeSeries {
    pub fn new(id: TimeSeriesId, meta_data: Labels) -> Self {
        TimeSeries {
            id,
            time_points: Vec::new(),
            meta_data,
        }
    }

    pub fn from_data(id: TimeSeriesId, meta_data: Labels, time_points: Vec<TimePoint>) -> Self {
        TimeSeries {
            id,
            time_points,
            meta_data,
        }
    }

    pub fn meta_data(&self) -> &Labels {
        &self.meta_data
    }

    pub fn add(&mut self, timestamp: Timestamp, value: Value) {
        self.time_points.push(TimePoint::new(timestamp, value))
    }

    pub fn time_points(&self) -> &Vec<TimePoint> {
        &self.time_points
    }

    pub fn id(&self) -> TimeSeriesId {
        self.id
    }
}


impl From<&TimeSeries> for crate::proto::TimeSeries {
    fn from(t: &TimeSeries) -> Self {
        crate::proto::TimeSeries {
            labels: t.meta_data.vec().iter().map(crate::proto::Label::from).collect(),
            samples: t.time_points.iter().map(Sample::from).collect(),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

impl From<&(Labels, Vec<TimePoint>)> for crate::proto::TimeSeries {
    fn from(t: &(Labels, Vec<TimePoint>)) -> Self {
        crate::proto::TimeSeries {
            labels: t.0.vec().iter().map(crate::proto::Label::from).collect(),
            samples: t.1.iter().map(Sample::from).collect(),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

impl From<&crate::proto::TimeSeries> for TimeSeries {
    fn from(t: &crate::proto::TimeSeries) -> Self {
        TimeSeries {
            id: 0,
            time_points: t.samples.iter().map(TimePoint::from).collect(),
            meta_data: Labels::from_vec(t.labels.to_vec().iter().map(Label::from).collect()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::common::label::Labels;
    use crate::common::time_series::TimeSeries;

    #[test]
    fn crate_time_series() {
        let _time_series = TimeSeries::new(12, Labels::new());
    }
}
