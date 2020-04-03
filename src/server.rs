use crate::MonolithDb;
use std::thread;
use crate::storage::Storage;
use crate::Result;
use crate::proto::{ReadRequest, ReadResponse, WriteRequest, QueryResult, Query};
use protobuf::{RepeatedField, Message, CodedInputStream, CodedOutputStream};
use crate::common::time_point::Timestamp;
use crate::common::label::{Labels, Label};
use crate::common::time_series::TimeSeries;
use tiny_http::{Server, Response, StatusCode, ResponseBox, Method, IncomingRequests, Request};
use std::io::{BufWriter, Write, Cursor, Read};
use log::Level;
use crate::indexer::Indexer;
use std::sync::Arc;
use rayon::ThreadPool;

pub const DEFAULT_PORT: i32 = 9001;
pub const DEFAULT_READ_PATH: &str = "/read";
pub const DEFAULT_WRITE_PATH: &str = "/write";
pub const DEFAULT_WORKER_NUM: usize = 8;


/// Http Server that accept Prometheus requests
///
/// Note that the Prometheus remote storage requests using __unframed__ snappy encoding __proto__ object.
///
pub struct MonolithServer<S, I>
    where S: Sync + Storage + Send + 'static,
          I: Sync + Indexer + Send + 'static {
    db: Arc<MonolithDb<S, I>>,
    port: i32,
    read_path: &'static str,
    write_path: &'static str,
    worker_num: usize,
}

impl<S, I> Clone for MonolithServer<S, I>
    where S: Sync + Send + Storage + 'static,
          I: Sync + Send + Indexer + 'static {
    fn clone(&self) -> Self {
        MonolithServer {
            db: Arc::clone(&self.db),
            port: self.port.clone(),
            read_path: &self.read_path.clone(),
            write_path: &self.write_path.clone(),
            worker_num: self.worker_num.clone(),
        }
    }
}

impl<S, I> MonolithServer<S, I>
    where S: Sync + Storage + Send + 'static,
          I: Sync + Indexer + Send + 'static {
    pub fn new(db: MonolithDb<S, I>) -> Self {
        MonolithServer {
            db: Arc::new(db),
            port: DEFAULT_PORT,
            read_path: DEFAULT_READ_PATH,
            write_path: DEFAULT_WRITE_PATH,
            worker_num: DEFAULT_WORKER_NUM,
        }
    }


    pub fn serve(self) -> Result<()> {
        let addr = format!("127.0.0.1:{}", self.port);
        let server = Server::http(addr).unwrap();

        let workers = rayon::ThreadPoolBuilder::new()
            .num_threads(self.worker_num)
            .build()
            .unwrap();


        for mut request in server.incoming_requests() {
            let server = self.clone();
            //do we need a context and a time out in case some thread stuck for some reason?
            workers.install(move || {
                MonolithServer::_process(server, request)
            });
        }

        Ok(())
    }

    fn _process(server: MonolithServer<S, I>, mut request: Request) {
        //Convert request content to protobuf coded format
        let mut content = Vec::new();
        request.as_reader().read_to_end(&mut content).unwrap();
        let mut decoder = snap::raw::Decoder::new();
        let _content = decoder.decompress_vec(content.as_slice()).unwrap();
        let mut _req_cur = Cursor::new(_content);
        let mut input_stream = CodedInputStream::new(&mut _req_cur);

        //triage the request
        match (request.method(), request.url()) {
            (_, read_path) if read_path == server.read_path => {
                let mut read_req = ReadRequest::new();
                let read = read_req.merge_from(&mut input_stream);
                if read.is_err() || read_req.compute_size() == 0 {
                    if read.is_err() {
                        error!("Cannot read content from read request {}", read.err().unwrap());
                    } else {
                        error!("Empty request")
                    }
                    request.respond(Response::empty(500));
                } else {
                    match server.query(read_req) {
                        Ok(read_res) => {
                            let mut _res_cur = Cursor::new(Vec::new());
                            let mut output_stream = CodedOutputStream::new(&mut _res_cur);
                            read_res.write_to(&mut output_stream);
                            output_stream.flush();
                            let mut _inner = Vec::new();
                            let pos = _res_cur.position();
                            _res_cur.set_position(0);
                            _res_cur.read_to_end(&mut _inner);
                            let mut encoder = snap::raw::Encoder::new();
                            let _res = encoder.compress_vec(_inner.as_slice()).unwrap();

                            let response = Response::from_data(_res.as_slice());

                            request.respond(response);
                        }
                        Err(err) => {
                            error!("{}", format!("Error when query against db, {}", err));
                            request.respond(Response::empty(500));
                        }
                    }
                }
            }
            (_, write_path) if write_path == server.write_path => {
                let mut write_req = WriteRequest::new();
                write_req.merge_from(&mut input_stream);
                match server.write(write_req) {
                    Ok(_) => {
                        request.respond(Response::empty(200));
                    }
                    Err(err) => {
                        error!("{}", format!("Error when write into db, {}", err));
                        request.respond(Response::empty(500));
                    }
                }
            }
            (_, _) => {
                request.respond(Response::empty(404));
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use crate::{Result, MonolithErr, MonolithDb, IdGenerator};
    use crate::storage::{Storage, SledStorage};
    use crate::common::time_point::{TimePoint, Timestamp};
    use crate::common::label::Labels;
    use crate::common::option::{DbOpts, StorageType};
    use crate::server::MonolithServer;
    use std::sync::RwLock;
    use crate::common::utils::get_current_timestamp;
    use crate::indexer::{SledIndexer, Indexer};
    use crate::common::time_series::TimeSeriesId;
    use crate::db::NewDb;

    struct StubStorage {}

    impl Storage for StubStorage {
        fn write_time_point(&self, time_series_id: u64, timestamp: u64, value: f64) -> Result<()> {
            unimplemented!()
        }

        fn read_time_series(&self, time_series_id: u64, start_time: u64, end_time: u64) -> Result<Vec<TimePoint>> {
            unimplemented!()
        }
    }

    struct StubIndexer {}

    impl Indexer for StubIndexer {
        fn get_series_with_label_matching(&self, labels: Labels) -> Result<Vec<(u64, Labels)>> {
            unimplemented!()
        }

        fn get_series_id_with_label_matching(&self, labels: Labels) -> Result<Vec<u64>> {
            unimplemented!()
        }

        fn get_series_id_by_labels(&self, labels: Labels) -> Result<Option<u64>> {
            unimplemented!()
        }

        fn create_index(&self, labels: Labels, time_series_id: u64) -> Result<()> {
            unimplemented!()
        }
    }

    impl crate::db::NewDb for MonolithDb<StubStorage, StubIndexer> {
        type S = StubStorage;
        type I = StubIndexer;

        fn get_storage_and_indexer(ops: DbOpts) -> Result<(Self::S, Self::I)> {
            Ok((
                StubStorage {},
                StubIndexer {}
            ))
        }
    }


    #[test]
    #[ignore]
    fn test_serve() -> Result<()> {
        env_logger::init();
        let opts = DbOpts::default();
        let db = MonolithDb::<StubStorage, StubIndexer>::new(opts)?;
        let server = MonolithServer::new(db);
        server.serve();


        Ok(())
    }
}
