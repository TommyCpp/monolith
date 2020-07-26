/// Prometheus remote write/read proto
/// Note that currently Prometheus will use HTTP with ProtocolBuffer as body to communicate.
mod remote;
mod types;

pub use remote::*;
pub use types::*;
