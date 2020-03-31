use clap::{App, Arg};
use monolith::indexer::SledIndexer;
use monolith::option::DbOpts;
use monolith::storage::SledStorage;
use monolith::db::NewDb;
use monolith::{MonolithDb, CHUNK_SIZE, DEFAULT_CHUNK_SIZE, FILE_DIR_ARG, STORAGE_ARG};
use monolith::server::MonolithServer;

#[macro_use]
extern crate log;

///
/// Binary command line wrapper for application
/// args:
/// storage, -s, default value sled, the storage type
///
fn main() {
    env_logger::init();

    let matches = App::new("monolith")
        .version(env!("CARGO_PKG_VERSION"))
        .about("time series storage")
        .args(&[
            Arg::with_name(STORAGE_ARG)
                .short("s")
                .long("storage")
                .default_value("sled"),
            Arg::with_name(FILE_DIR_ARG)
                .short("dir")
                .default_value(env!("PWD")),
            Arg::with_name(CHUNK_SIZE)
                .short("c")
                .default_value(DEFAULT_CHUNK_SIZE),
        ])
        .get_matches();

    let options = DbOpts::get_config(matches).expect("Cannot read config");

    let db: MonolithDb<SledStorage, SledIndexer> = MonolithDb::<SledStorage, SledIndexer>::new(options).unwrap();
    let server = MonolithServer::new(db);
    server.serve();
}
