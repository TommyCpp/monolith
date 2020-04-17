use clap::{App, Arg};

use monolith::indexer::{SledIndexer, SledIndexerBuilder};
use monolith::option::{DbOpts, ServerOpts};
use monolith::storage::{SledStorage, SledStorageBuilder};
use monolith::{MonolithDb, CHUNK_SIZE, DEFAULT_CHUNK_SIZE, FILE_DIR_ARG, STORAGE_ARG, PORT, DEFAULT_PORT, WRITE_PATH, DEFAULT_WRITE_PATH, READ_PATH, DEFAULT_READ_PATH, WORKER_NUM, DEFAULT_WORKER_NUM};
use monolith::server::MonolithServer;
use std::sync::Arc;


#[macro_use]
extern crate log;

type SledMonolithDb = MonolithDb<SledStorage, SledIndexer>;

///
/// Binary command line wrapper for application
/// args:
/// storage, -s, default value sled, the storage type
///
fn main() {
    env_logger::init();

    let default_port = DEFAULT_PORT.to_string();
    let default_worker_num = DEFAULT_WORKER_NUM.to_string();

    let matches = App::new("monolith")
        .version(env!("CARGO_PKG_VERSION"))
        .about("time series storage")
        .args(&[
            Arg::with_name(STORAGE_ARG)
                .short("s")
                .long(STORAGE_ARG)
                .default_value("sled"),
            Arg::with_name(FILE_DIR_ARG)
                .short("dir")
                .default_value(env!("PWD")),
            Arg::with_name(CHUNK_SIZE)
                .long(CHUNK_SIZE)
                .default_value(DEFAULT_CHUNK_SIZE),
            Arg::with_name(PORT)
                .long(PORT)
                .default_value(default_port.as_str()),
            Arg::with_name(WRITE_PATH)
                .long(WRITE_PATH)
                .default_value(DEFAULT_WRITE_PATH),
            Arg::with_name(READ_PATH)
                .long(READ_PATH)
                .default_value(DEFAULT_READ_PATH),
            Arg::with_name(WORKER_NUM)
                .long(WORKER_NUM)
                .default_value(default_worker_num.as_str())
        ])
        .get_matches();

    let db_opts = DbOpts::get_config(&matches).expect("Cannot read db config");
    let server_opts = ServerOpts::get_config(&matches).expect("Cannot read server config");

    info!("{}", server_opts);

    let storage_builder = SledStorageBuilder::new();
    let indexer_builder = SledIndexerBuilder::new();

    let db: Arc<SledMonolithDb> = SledMonolithDb::new(db_opts, Box::new(storage_builder), Box::new(indexer_builder)).unwrap();

    let server = MonolithServer::new(server_opts, db);
    let _ = server.serve();
}
