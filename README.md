# monolith
![Rust](https://github.com/TommyCpp/monolith/workflows/Rust/badge.svg)

timeseries database. It's still a **WIP** project(Including this README).

## Usage
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

`Chunk` is basic component that contains data of a small range of times. Each `Chunk` has it's own dictionary. `Chunk` is mainly consisted by `Indexer` and `Storage`.   

2. Storage

`Storage` stores `(timestamp, value)` pair. `Storage` doesn't store `label` information. All data will be reference by an generated id.

3. Indexer

`Indexer` stores `label` information and generated id. User should use `Indexer` to find the target time series's id. And use this id to query data from `Storage`


## Storage options
### Sled
Sled is a embedding key-value database. The API of Sled is similar with BTreeMap or any other map. 

## TODO List
- [ ] Add metadata file in base dir
- [ ] Add unit tests
- [ ] Add e2e tests with Prometheus
- [x] Add CI/CD pipeline
