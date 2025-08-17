# Alethic ISM State Tables Processor

## Overview

A Go microservice that manages batched database table operations within the Alethic ISM ecosystem. It processes incoming messages and writes them to dynamically created tables with configurable batching based on size and time thresholds.

## Core Functionality

- **Dynamic Table Creation**: Automatically creates tables based on incoming data schema
- **Batch Processing**: Configurable batching by size and time window
- **Automatic Flushing**: Time-based and size-based flush triggers
- **Memory Management**: Idle manager cleanup for inactive processors

## Architecture

### Components

1. **Message Layer** (`main.go`, `pkg/handler/`)
   - NATS subscriber for incoming route messages
   - Publishes status updates to processor monitor
   - Graceful shutdown with signal handling

2. **Table Manager** (`pkg/handler/table_manager.go`)
   - **BatchWriter**: Manages batched inserts with auto-flush
   - **Manager Cache**: Maintains processor-specific writers
   - **Background Flush**: Periodic flush based on TTL

3. **Data Layer** (`pkg/handler/database.go`)
   - PostgreSQL for dynamic table creation
   - Schema inference from incoming data
   - Connection pooling and transaction management

## Batch Algorithm

1. Receive incoming message with route data
2. Get or create BatchWriter for processor
3. Add records to batch with optional timestamps
4. Flush if batch size threshold reached
5. Background goroutine flushes on time intervals
6. Create table on first flush using schema

## Configuration

### Environment Variables
- `DSN`: PostgreSQL connection string

### Processor Properties
```json
{
  "tableName": "custom_table",
  "batchSize": 100,
  "batchWindowTTL": 30,
  "includeTimestamp": true
}
```

## Building

```bash
go build -o main .
```

## Docker

```bash
docker build -t alethic-ism-state-tables .
```

## Dependencies

- Go 1.24
- `github.com/quantumwake/alethic-ism-core-go`
- PostgreSQL
- NATS Server

## License

Dual licensing model - AGPL v3 for open source, commercial license available. See [LICENSE.md](LICENSE.md).
