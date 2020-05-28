## Environment Setup
TODO
## Components
1. Chunk

`Chunk` is basic component that contains data of a small range of times. Each `Chunk` has it's own dictionary. `Chunk` mainly consists `Indexer` and `Storage`.   

2. Storage

`Storage` stores `(timestamp, value)` pair. `Storage` doesn't store `label` information. All data will be reference by an generated id.

3. Indexer

`Indexer` stores `label` information and generated id. User should use `Indexer` to find the target time series's id. And use this id to query data from `Storage`