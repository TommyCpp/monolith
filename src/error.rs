use std::error::Error;

pub enum MonolithErr {}

pub type Result<T> = std::result::Result<T, MonolithErr>;