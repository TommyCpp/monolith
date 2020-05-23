# monolith-rs
![Rust](https://github.com/TommyCpp/monolith/workflows/Rust/badge.svg)
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)


Timeseries database. It could be used as an storage backend for prometheus to store metric data. 

It's still a **WIP** project(Including this README).

## Usage
Start a server as remote write and read endpoint of Prometheus.
```shell script
monolith-server [Options]
    
    -s   --storage    type of storage.
    -dir --file_dir   dictionary of data.
         --chunk_size size of one chunk in seconds.
         --port       port of server.
         --write_path path of write endpoint. 
         --read_path  path of read endpoint.
         --worker_num number of thread to process incoming requests.
```

## Components
1. Chunk

`Chunk` is basic component that contains data of a small range of times. Each `Chunk` has it's own dictionary. `Chunk` mainly consists `Indexer` and `Storage`.   

2. Storage

`Storage` stores `(timestamp, value)` pair. `Storage` doesn't store `label` information. All data will be reference by an generated id.

3. Indexer

`Indexer` stores `label` information and generated id. User should use `Indexer` to find the target time series's id. And use this id to query data from `Storage`


## Storage options
### Sled
Sled is a embedding key-value database. The API of Sled is similar with BTreeMap or any other map. 

### TiKV
Tikv is a distributed transactional database. TiKV can be used as **shard** backend for both `Indexer` and `Storage`. `Chunk` will attach appropriate tag to distinguished data belong to which component. 

## TODO List
- [x] Add metadata file in base dir
- [ ] Compression on swap chunk
- [ ] Add unit tests
- [ ] Add e2e tests with Prometheus
- [x] Add CI/CD pipeline
- [x] Add more storage options
