use monolith::{MonolithServer, ServerOps, StorageType, Result};
use clap::{App, Arg, ArgMatches};
use std::env::args;

const STORAGE_ARG: &str = "storage";

fn main() {
    let server: MonolithServer = MonolithServer::new(None).unwrap();
    let matches = App::new("monolith")
        .version(env!("CARGO_PKG_VERSION"))
        .about("time series storage")
        .args(
            &[
                Arg::with_name(STORAGE_ARG)
                    .short("s")
                    .long("storage")
            ]
        )
        .get_matches();
}

fn get_config(matches: ArgMatches) -> Resuclt<ServerOps> {
    let storage_type = match matches.value_of(STORAGE_ARG).unwrap() {
        "sledge" => StorageType::SledgeStorage,
        _ => StorageType::UnknownStorage,
    };
    Ok(
        ServerOps {
            storage: storage_type
        }
    )
}
