use failure::_core::convert::Infallible;
use failure::_core::num::{ParseFloatError, ParseIntError};

use std::string::FromUtf8Error;
use std::sync::Arc;
use crate::storage::Storage;
use crate::indexer::Indexer;
use crate::chunk::Chunk;
use serde_json::Error;

#[derive(Debug, Fail)]
pub enum MonolithErr {
    #[fail(display = "Io Error: {}", _0)]
    IoError(String),
    #[fail(display = "Option error")]
    OptionErr,
    #[fail(display = "Parse error")]
    ParseErr,
    #[fail(display = "Not found")]
    NotFoundErr,
    /// Out of the target range, the two param shows the target range.
    #[fail(display = "Out of range, target range is {}, {}", _0, _1)]
    OutOfRangeErr(u64, u64),
    #[fail(display = "Internal error, {}", _0)]
    InternalErr(String),
    // Logical error, usually indicate a bug inside system
    #[fail(display = "Error when convert Json with serde")]
    SerdeJsonErr(serde_json::error::Error),
    #[fail(display = "Error when convert Yaml with serde")]
    SerdeYamlErr(serde_yaml::Error),
    #[fail(display = "Error from TiKv")]
    TiKvErr(tikv_client::Error),
}

pub type Result<T> = std::result::Result<T, MonolithErr>;

impl std::convert::From<std::io::Error> for MonolithErr {
    fn from(err: std::io::Error) -> Self {
        MonolithErr::IoError(err.to_string())
    }
}

impl std::convert::From<std::convert::Infallible> for MonolithErr {
    //didn't implement because Infallible should never happen
    fn from(_: Infallible) -> Self {
        unimplemented!()
    }
}

impl std::convert::From<std::num::ParseIntError> for MonolithErr {
    fn from(_: ParseIntError) -> Self {
        MonolithErr::ParseErr
    }
}

impl std::convert::From<sled::Error> for MonolithErr {
    fn from(err: sled::Error) -> Self {
        MonolithErr::IoError(format!(
            "Cannot start Sled Storage because {}",
            err.to_string()
        ))
    }
}

impl std::convert::From<FromUtf8Error> for MonolithErr {
    fn from(_: FromUtf8Error) -> Self {
        MonolithErr::ParseErr
    }
}

impl std::convert::From<std::num::ParseFloatError> for MonolithErr {
    fn from(_: ParseFloatError) -> Self {
        MonolithErr::ParseErr
    }
}

impl<S: Storage, I: Indexer> std::convert::From<std::sync::Arc<Chunk<S, I>>> for MonolithErr {
    fn from(_: Arc<Chunk<S, I>>) -> Self {
        MonolithErr::InternalErr("Cannot get ownership".to_string())
    }
}

impl std::convert::From<serde_json::error::Error> for MonolithErr {
    fn from(err: Error) -> Self {
        MonolithErr::SerdeJsonErr(err)
    }
}

impl std::convert::From<tikv_client::Error> for MonolithErr {
    fn from(err: tikv_client::Error) -> Self {
        MonolithErr::TiKvErr(err)
    }
}

impl std::convert::From<serde_yaml::Error> for MonolithErr{
    fn from(err: serde_yaml::Error) -> Self {
        MonolithErr::SerdeYamlErr(err)
    }
}
