use crate::{MonolithDb, Indexer};
use crate::storage::Storage;
use crate::Result;
use crate::proto::{ReadRequest, ReadResponse, WriteRequest, QueryResult, Query};
use protobuf::RepeatedField;
use crate::common::time_point::Timestamp;
use crate::common::label::{Labels, Label};
use crate::common::time_series::TimeSeries;


pub const DEFAULT_PORT: i32 = 9001;
pub const READ_PATH: &str = "/read";
pub const WRITE_PATH: &str = "/write";


pub struct MonolithServer<'a, S: Storage, I: Indexer> {
    db: MonolithDb<S, I>,
    port: i32,
    read_path: &'a str,
    write_path: &'a str,
}

impl<S: Storage, I: Indexer> MonolithServer<'static, S, I> {
    pub fn new(db: MonolithDb<S, I>) -> Self {
        MonolithServer {
            db,
            port: 9001,
            read_path: "/read",
            write_path: "/write",
        }
    }

    pub fn serve(self) -> Result<()> {
        unimplemented!();
    }

    //todo: test it
    pub fn query(&self, read_rq: ReadRequest) -> Result<ReadResponse> {
        Ok(ReadResponse {
            results: RepeatedField::from(
                read_rq.queries.iter()
                    .map(|q|
                        self.db.query(
                            Labels::from_vec(
                                q.matchers.iter()
                                    .map(Label::from_label_matcher)
                                    .collect::<Vec<Label>>()
                            ),
                            q.start_timestamp_ms as Timestamp,
                            q.end_timestamp_ms as Timestamp)
                            .ok().unwrap_or(Vec::new())
                            .iter()
                            .map(crate::proto::TimeSeries::from)
                            .collect::<Vec<crate::proto::TimeSeries>>()
                    )
                    .map(|v: Vec<crate::proto::TimeSeries>| -> QueryResult{
                        QueryResult {
                            timeseries: RepeatedField::from(v),
                            unknown_fields: Default::default(),
                            cached_size: Default::default(),
                        }
                    })
                    .collect::<Vec<crate::proto::QueryResult>>()
            ),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        })
    }

    pub fn write(&self, write_rq: WriteRequest) -> Result<()> {
        for time_series in write_rq.timeseries.to_vec() {
            let _ts: TimeSeries = TimeSeries::from(&time_series);
            self.db.write_time_points(_ts.meta_data().clone(), _ts.time_points().clone())?
        }

        Ok(())
    }
}