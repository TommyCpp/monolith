use std::error::Error;
use failure::_core::convert::Infallible;
use failure::_core::num::{ParseIntError, ParseFloatError};
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum MonolithErr {
    IoError(String),
    OptionErr,
    ParseErr,
    NotFoundErr,
    OutOfRangeErr(u64, u64),
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
        MonolithErr::IoError(format!("Cannot start Sled Storage because {}", err.to_string()))
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