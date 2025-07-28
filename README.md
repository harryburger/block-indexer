# Block Injector

A multichain event indexer implemented in Rust, designed to consume on-chain `Transfer` and `DelegateVotesChanged` events, persist raw event data, and maintain member transaction and balance state.

## Features

- **Multi-chain Support**: Ethereum, Polygon, Arbitrum, Sepolia, Base
- **Event Processing**: Transfer and DelegateVotesChanged events
- **Historical & Real-time Sync**: Both batch processing and live monitoring
- **Deduplication**: Prevents duplicate event processing
- **State Management**: Tracks sync progress per network/token
- **RESTful API**: HTTP endpoints for triggering syncs and monitoring status

## Architecture

```
+----------------+        +-------------+       +--------------+
|  HTTP API      |        |    Queue    |       |   Worker(s)  |
|  (Sync trigger)| -----> | (In-memory) | ----> |   Processor  |
+----------------+        +-------------+       +--------------+
        |                                           |
        | REST endpoints                            | Pull from queue
        v                                           v
+----------------+    Poller    +-------------+    +------------------+
|  Rust Service  | <----------> |  Node JSON-RPC| -> | Event Handlers   |
| - Config       |              +-------------+    | - Transfer       |
| - MongoClient  |                                 | - DelegateVotesChanged |
+----------------+                                 +------------------+
```

## Quick Start

### Prerequisites

- Rust (latest stable)
- MongoDB (running on localhost:27017)
- Blockchain RPC endpoints (Alchemy, Infura, etc.)

### Setup

1. **Clone and build the project:**
   ```bash
   cargo build
   ```

2. **Configure RPC endpoints:**
   Edit `config.yaml` and replace `YOUR_API_KEY` with your actual API keys:
   ```yaml
   chains:
     ethereum:
       rpc_url: "https://eth-mainnet.g.alchemy.com/v2/YOUR_ACTUAL_API_KEY"
   ```

3. **Start MongoDB:**
   ```bash
   # Using Docker
   docker run -d -p 27017:27017 --name mongodb mongo:latest
   
   # Or use your local MongoDB installation
   mongod
   ```

4. **Run the application:**
   ```bash
   RUST_LOG=info cargo run
   ```

The service will start on `http://localhost:3000`.

## API Endpoints

### POST /sync/historical
Trigger a historical sync for a specific token and block range.

**Request:**
```json
{
  "network": "ethereum",
  "tokenAddress": "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0",
  "fromBlock": 18000000,
  "toBlock": 18001000
}
```

**Response:**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued"
}
```

### GET /status/{jobId}
Get the status of a sync job.

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "progress": 1.0,
  "error": null
}
```

### GET /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-07-27T01:35:00Z"
}
```

## Configuration

The application uses `config.yaml` for configuration:

```yaml
app:
  mongodb_url: "mongodb://localhost:27017"
  mongodb_database: "block_injector"

chains:
  ethereum:
    enabled: true
    rpc_url: "https://eth-mainnet.g.alchemy.com/v2/..."
    poll_interval_seconds: 6
    finality_wait_seconds: 30

topics:
  # Transfer event
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "Transfer"
  # DelegateVotesChanged event  
  "0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724": "DelegateVotesChanged"
```

## Data Models

### MongoDB Collections

- **rawEvents**: Raw blockchain event data
- **memberTx**: Processed member transactions
- **memberBalances**: Current member balances and voting power
- **configIndexers**: Sync state tracking
- **syncJobs**: Job status and history

## Development

### Adding New Event Handlers

1. Create a new handler implementing the `EventHandler` trait:
   ```rust
   pub struct MyCustomHandler;
   
   impl EventHandler for MyCustomHandler {
       async fn handle(&self, event: RawEventData, db: &DatabaseClient) -> Result<(), HandlerError> {
           // Process the event
           Ok(())
       }
   }
   ```

2. Register the handler in `handlers/mod.rs`:
   ```rust
   pub fn get_handler_for_topic(topic_hash: &str) -> Option<Box<dyn EventHandler + Send + Sync>> {
       match topic_hash {
           "0x..." => Some(Box::new(MyCustomHandler)),
           // ... existing handlers
       }
   }
   ```

3. Add the topic to `config.yaml`.

### Running Tests

```bash
cargo test
```

### Logging

Set the log level using the `RUST_LOG` environment variable:
```bash
RUST_LOG=debug cargo run
```

## Production Deployment

### Environment Variables

- `RUST_LOG`: Log level (info, debug, warn, error)
- `CONFIG_PATH`: Path to config file (default: "config")

### Docker Deployment

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/block-injector /usr/local/bin/
COPY --from=builder /app/config.yaml /etc/block-injector/
CMD ["block-injector"]
```

### Scaling Considerations

- **Horizontal Scaling**: Multiple processor instances can share the same queue
- **Database Sharding**: Partition by network or token for large deployments
- **Rate Limiting**: Configure RPC rate limits per provider
- **Monitoring**: Use the `/health` endpoint for health checks

## License

MIT License - see LICENSE file for details.
