use std::error::Error;

#[derive(Debug)]
pub enum MonolithErr {
    IoError(std::io::Error)
}

pub type Result<T> = std::result::Result<T, MonolithErr>;

impl std::convert::From<std::io::Error> for MonolithErr {
    fn from(err: std::io::Error) -> Self {
        MonolithErr::IoError(err)
    }
}