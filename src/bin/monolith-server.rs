#![allow(unused_must_use)]

use clap::{Arg, Command};

use monolith::indexer::*;
use monolith::option::{DbOpts, ServerOpts};
use monolith::server::MonolithServer;
use monolith::storage::*;
use monolith::*;
use std::process::exit;

#[macro_use]
extern crate log;

///
/// Binary command line wrapper for application
/// args:
/// storage, -s, default value sled, the storage type
///
fn main() {
    env_logger::init();

    let default_port = DEFAULT_PORT.to_string();
    let default_worker_num = DEFAULT_WORKER_NUM.to_string();

    let matches = Command::new("monolith")
        .version(env!("CARGO_PKG_VERSION"))
        .about("time series storage")
        .args([
            Arg::new(STORAGE_ARG)
                .short('s')
                .long(STORAGE_ARG)
                .default_value(SLED_BACKEND),
            Arg::new(INDEXER_ARG)
                .short('i')
                .long(INDEXER_ARG)
                .default_value(SLED_BACKEND),
            Arg::new(FILE_DIR_ARG)
                .short('d')
                .long(FILE_DIR_ARG)
                .default_value(env!("PWD")),
            Arg::new(CHUNK_SIZE)
                .long(CHUNK_SIZE)
                .default_value(DEFAULT_CHUNK_SIZE),
            Arg::new(PORT)
                .long(PORT)
                .default_value(default_port),
            Arg::new(WRITE_PATH)
                .long(WRITE_PATH)
                .default_value(DEFAULT_WRITE_PATH),
            Arg::new(READ_PATH)
                .long(READ_PATH)
                .default_value(DEFAULT_READ_PATH),
            Arg::new(WORKER_NUM)
                .long(WORKER_NUM)
                .default_value(default_worker_num),
            Arg::new(TIKV_CONFIG).long(TIKV_CONFIG),
        ])
        .get_matches();

    let db_opts = DbOpts::get_config(&matches).expect("Cannot read db config");
    let server_opts = ServerOpts::get_config(&matches).expect("Cannot read server config");

    info!("Server options: \n {}", server_opts);

    let tikv_backend_singleton = if db_opts.tikv_config.is_some()
        && (db_opts.indexer == TIKV_BACKEND || db_opts.storage == TIKV_BACKEND)
    {
        // if tikv config is provided and storage or indexer is tikv based
        // read config file
        Some(
            TiKvRawBackendSingleton::from_config_file(
                db_opts.tikv_config.clone().unwrap().as_path(),
            )
            .unwrap(),
        )
    } else {
        if db_opts.indexer == TIKV_BACKEND || db_opts.storage == TIKV_BACKEND {
            warn!("No TiKV config file provided, will use dry run mode; if you want to persist your data, please provide a tikv config file");
            Some(TiKvRawBackendSingleton::default())
        } else {
            None
        }
    };

    macro_rules! build_storage {
        (TiKV) => {
            TiKvStorageBuilder::new(tikv_backend_singleton.clone().unwrap()).unwrap()
        };
        (Sled) => {
            SledStorageBuilder::new()
        };
    }

    macro_rules! build_indexer {
        (TiKV) => {
            TiKvIndexerBuilder::new(tikv_backend_singleton.clone().unwrap()).unwrap()
        };
        (Sled) => {
            SledIndexerBuilder::new()
        };
    }

    macro_rules! start_server {
        ($storage:ty, $indexer:ty, $storage_builder: ident, $indexer_builder: ident) => {
            let db = MonolithDb::<$storage, $indexer>::new(
                db_opts,
                Box::new($storage_builder),
                Box::new($indexer_builder),
            )
            .unwrap();
            let server = MonolithServer::new(server_opts, db);
            server.serve();
        };
    }

    match db_opts.storage.as_str() {
        TIKV_BACKEND => match db_opts.indexer.as_str() {
            TIKV_BACKEND => {
                let storage_builder = build_storage!(TiKV);
                let indexer_builder = build_indexer!(TiKV);
                start_server!(TiKvStorage, TiKvIndexer, storage_builder, indexer_builder);
            }
            SLED_BACKEND => {
                let storage_builder = build_storage!(TiKV);
                let indexer_builder = build_indexer!(Sled);
                start_server!(TiKvStorage, SledIndexer, storage_builder, indexer_builder);
            }
            _ => exit(1),
        },
        SLED_BACKEND => match db_opts.indexer.as_str() {
            TIKV_BACKEND => {
                let storage_builder = build_storage!(Sled);
                let indexer_builder = build_indexer!(TiKV);
                start_server!(SledStorage, TiKvIndexer, storage_builder, indexer_builder);
            }
            SLED_BACKEND => {
                let storage_builder = build_storage!(Sled);
                let indexer_builder = build_indexer!(Sled);
                start_server!(SledStorage, SledIndexer, storage_builder, indexer_builder);
            }
            _ => exit(1),
        },
        _ => exit(1),
    };
}
