use std::error::Error;

#[derive(Debug)]
pub enum MonolithErr {}

pub type Result<T> = std::result::Result<T, MonolithErr>;