use std::error::Error;
use failure::_core::convert::Infallible;
use failure::_core::num::ParseIntError;

#[derive(Debug)]
pub enum MonolithErr {
    IoError(std::io::Error),
    OptionErr,
    ParseError,
}

pub type Result<T> = std::result::Result<T, MonolithErr>;

impl std::convert::From<std::io::Error> for MonolithErr {
    fn from(err: std::io::Error) -> Self {
        MonolithErr::IoError(err)
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
        MonolithErr::ParseError
    }
}