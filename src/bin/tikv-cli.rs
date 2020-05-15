use monolith::{TiKvRawBackendSingleton, DEFAULT_PORT, DEFAULT_READ_PATH, MonolithDb};
use tikv_client::Config;
use monolith::storage::{TiKvStorageBuilder, TiKvStorage};
use monolith::indexer::{TiKvIndexer, TiKvIndexerBuilder};
use monolith::option::{ServerOpts, DbOpts};
use monolith::server::MonolithServer;
use std::sync::Arc;

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


    let db= MonolithDb::<TiKvStorage, TiKvIndexer>::new(db_opts,
                                    Box::new(storage_builder),
                                    Box::new(indexer_builder)).unwrap();
    let server = MonolithServer::new(server_opts, db);
    let _ = server.serve();
}