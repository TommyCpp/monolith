use monolith::{MonolithServer, ServerOps, StorageType, Result, get_config, STORAGE_ARG, FILE_DIR_ARG};
use clap::{App, Arg, ArgMatches};
use std::env::args;
use std::path::PathBuf;
use std::str::FromStr;

fn main() {
    let matches = App::new("monolith")
        .version(env!("CARGO_PKG_VERSION"))
        .about("time series storage")
        .args(
            &[
                Arg::with_name(STORAGE_ARG)
                    .short("s")
                    .long("storage")
                    .default_value("sled"),
                Arg::with_name(FILE_DIR_ARG)
                    .short("dir")
                    .default_value(env!("PWD"))
            ]
        )
        .get_matches();

    let options = get_config(matches).expect("Cannot read config");

    let server: MonolithServer = MonolithServer::new(options).unwrap();
}
