#![allow(unused_must_use)]
#![allow(dead_code)]

use monolith::indexer::{TiKvIndexer, TiKvIndexerBuilder};
use monolith::option::{DbOpts, ServerOpts};
use monolith::server::MonolithServer;
use monolith::storage::{TiKvStorage, TiKvStorageBuilder};
use monolith::{MonolithDb, TiKvRawBackendSingleton, DEFAULT_PORT, DEFAULT_READ_PATH};
use std::sync::Arc;
use tikv_client::Config;

/// Simple command line tool to connect to Tikv
fn main() {
    //todo: add Dockerfile
    //todo: test it
    println!("Starting Db");
    let config = Config::new(vec!["http://127.0.0.1:2379"]);
    let builder = TiKvRawBackendSingleton::new(config).unwrap();
    let backend = builder.get_instance().unwrap();

    let storage_builder = TiKvStorageBuilder::new(builder.clone()).unwrap();
    let indexer_builder = TiKvIndexerBuilder::new(builder).unwrap();

    let server_opts = ServerOpts::default();
    let db_opts = DbOpts::default();

    let db = MonolithDb::<TiKvStorage, TiKvIndexer>::new(
        db_opts,
        Box::new(storage_builder),
        Box::new(indexer_builder),
    )
    .unwrap();
    let server = MonolithServer::new(server_opts, db);
    let _ = server.serve();
}
