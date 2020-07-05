# monolith-rs
![Rust](https://github.com/TommyCpp/monolith/workflows/Rust/badge.svg)
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)


Timeseries database. It could be used as an storage backend for prometheus to store metric data. 

It's still a **WIP** project(Including this README).

### Usage
Start a server as remote write and read endpoint of Prometheus.
```shell script
monolith-server [Options]
    
    -s   --storage    type of storage. Valid value <sled, tikv>
    -i   --indexer    type of indexer. Valid value <sled, tikv>
    -dir --file_dir   dictionary of to store the data from indexer and storage.
         --chunk_size size of one chunk in seconds.
         --port       port of server. Default to be 10090
         --write_path path of write endpoint. Default to be /write
         --read_path  path of read endpoint. Default to be /read
         --worker_num number of thread to process incoming requests. We use multiple 
         --tikv_config config file if using tikv backend. If not provide, will go into debug mode and use an in-memory key value map to mock tikv.
```

After started monolith-server, you can set remote write endpoint in prometheus to be `http://127.0.0.1:10090/write` if using default port.

To connect to tikv, provide below information in yaml file and use that file in `--tikv_config`

```yaml
dry_run: false # if true, then instead of connecting to tikv, we will use an in-memory key value map to mock tikv. Default to be true
pd_endpoint: # pd endpoint of tikv cluster. 
 - 127.0.0.1 
```

If user want to test with tikv locally, it's recommended to use [binary deployment](https://tikv.org/docs/3.0/tasks/deploy/binary/) to avoid docker network issue.

### Storage options
For now, user can choose between two different kinds of backend for both storage or indexer. user can also use different backend for storage and indexer. To do so, just specific different type in command line argument.

#### Sled
Sled is a embedding key-value database. The API of Sled is similar with BTreeMap or any other map. 

#### TiKV
Tikv is a distributed transactional database. TiKV can be used as **shared** backend for both `Indexer` and `Storage`. `Chunk` will attach appropriate tag to distinguished data belong to which component. 

### Development
See [here](https://github.com/TommyCpp/monolith/tree/master/doc/development.md)

### TODO List
- [x] Add metadata file in base dir
- [ ] Compression on swap chunk
- [ ] Add unit tests
- [ ] Add e2e tests with Prometheus
- [x] Add CI/CD pipeline
- [x] Add more storage options
