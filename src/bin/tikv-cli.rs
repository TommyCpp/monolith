use monolith::{TiKvRawBackendBuilder, Builder};
use tikv_client::Config;

/// Simple command line tool to connect to Tikv
fn main() {
    //todo: add Dockerfile
    let config = Config::new(vec!["http://pd.tikv:2379", "http://pd.tikv:2380"]);
    let builder = TiKvRawBackendBuilder::new(config).unwrap();
    let backend = builder.build("".into()).unwrap();
    backend.set(vec![8 as u8], vec![117 as u8]).unwrap();
    let res: Vec<u8> = backend.get(vec![8 as u8]).unwrap().unwrap();

    print!("{}", std::str::from_utf8(res.as_slice()).unwrap())
}