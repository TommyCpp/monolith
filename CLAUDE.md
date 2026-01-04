# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Monolith is a Rust-based time series database (TSDB) designed as a Prometheus-compatible remote storage backend. It supports pluggable storage and indexer backends (Sled for embedded, TiKV for distributed deployments).

## Build Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
cargo test <test_name>   # Run a specific test
cargo bench              # Run benchmarks (criterion)
cargo check              # Quick syntax/type checking
```

## Running the Server

```bash
# With Sled backend (embedded)
cargo run --bin monolith-server -- -s sled -i sled --file_dir /data/monolith --port 9001

# With TiKV backend (distributed)
cargo run --bin monolith-server -- -s tikv -i tikv --tikv_config config.yaml
```

## Architecture

### Core Data Flow

```
MonolithServer (HTTP) → MonolithDb → Chunk(s) → Storage + Indexer
```

- **MonolithServer** (`src/server.rs`): HTTP handler for Prometheus remote write/read protocol
- **MonolithDb** (`src/db.rs`): Thread-safe database orchestrator, manages chunk lifecycle
- **Chunk** (`src/chunk/`): Time-bounded data container, owns a Storage and Indexer instance

### Trait Abstractions

Storage and Indexer are defined as traits with multiple implementations:

- **Storage trait** (`src/storage/common.rs`): Persists TimePoint data (timestamp + value pairs)
  - `SledStorage`: Embedded key-value store
  - `TiKvStorage`: Distributed database

- **Indexer trait** (`src/indexer/common.rs`): Maps Labels to TimeSeriesIds
  - `SledIndexer`: Embedded key-value store
  - `TiKvIndexer`: Distributed database

### Key Data Types

- **TimePoint**: `(u64 timestamp, f64 value)` - single metric sample
- **TimeSeries**: Collection of TimePoints with associated Labels
- **TimeSeriesId**: Unique identifier linking Indexer entries to Storage data
- **Label**: Key-value pair for metric metadata (e.g., `job="prometheus"`)

### Supporting Components

- **Compaction** (`src/compaction/`): Data compression using Gorilla algorithm
- **WAL** (`src/wal/`): Write-ahead log for crash recovery (indexes only; timepoint loss acceptable)
- **Proto** (`src/proto/`): Generated Protobuf code for Prometheus protocol

## Key Design Decisions

1. **Separation of Indexer and Storage**: Labels stored in Indexer; time points in Storage. Queries first resolve TimeSeriesIds via Indexer, then fetch data from Storage.

2. **Time-based Chunking**: Data organized into time windows. Current chunk is mutable; older chunks are read-only and compacted.

3. **Backend Flexibility**: Storage and Indexer backends can be mixed (e.g., Sled indexer with TiKV storage).

4. **Thread Safety**: Uses `Arc<RwLock<>>` for concurrent access to chunks and database state.

## Test Organization

Integration tests are in `tests/`:
- `test_chunk.rs` - Chunk operations
- `test_sled_indexer.rs` - Sled indexer functionality
- `test_sled_storage.rs` - Sled storage operations
- `test_cli.rs` - CLI argument parsing
- `test_server.rs` - Server functionality

Test utilities are in `src/common/test_utils.rs`.
