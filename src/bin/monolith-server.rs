use clap::{App, Arg, ArgMatches};
use monolith::option::{get_config, ServerOps, StorageType};
use monolith::{MonolithServer, Result, CHUNK_SIZE, DEFAULT_CHUNK_SIZE, FILE_DIR_ARG, STORAGE_ARG};
use std::env::args;
use std::path::PathBuf;
use std::str::FromStr;

fn main() {
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

    let options = get_config(matches).expect("Cannot read config");

    let server: MonolithServer = MonolithServer::new(options).unwrap();
}
