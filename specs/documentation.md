## 1. Overview

A multichain event indexer implemented in Rust, designed to consume on-chain `Transfer` and `DelegateVotesChanged` (and any other topics defined in configuration), persist raw event data, maintain member transaction and balance state, and provide both bulk (historical) and realtime indexing.

### Goals

* **Historical sync**: Batch-fetch and index events from a specified block range per token and network.
* **Realtime indexing**: Poll new blocks at a configurable interval, filter and process incoming events.
* **Deduplication**: Ensure no event is processed twice, even across restarts.
* **Stateful**: Track last-synced block/transaction/log per token & network.
* **Extensible**: Support arbitrary topics via config, with built‑in handlers for transfer and delegate.

---

## 2. High-Level Architecture

```text
+----------------+        +-------------+       +--------------+
|  HTTP API      |        |    Queue    |       |   Worker(s)  |
|  (Sync trigger)| -----> | (Rabbit/Kafka)| --> |   Processor  |
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

---

## 3. Configuration (config.yaml)

```yaml
app:
  name: block-injector
  version: "0.1.0"
  health_check_interval_ms: 15000
  summary_interval_seconds: 30
  elasticsearch_url: "http://localhost:9200"
  elasticsearch_index: "blockchain_data"
  elasticsearch_batch_size: 200

chains:
  ethereum:
    enabled: true
    rpc_url: "https://eth-mainnet.g.alchemy.com/v2/..."
    poll_interval_seconds: 6
    finality_wait_seconds: 30
  polygon:
    enabled: true
    rpc_url: "https://polygon-mainnet.g.alchemy.com/v2/..."
    poll_interval_seconds: 3
    finality_wait_seconds: 10
  arbitrum:
    enabled: true
    rpc_url: "https://arb-mainnet.g.alchemy.com/v2/..."
    poll_interval_seconds: 3
    finality_wait_seconds: 10
  sepolia:
    enabled: true
    rpc_url: "https://eth-sepolia.g.alchemy.com/v2/..."
    poll_interval_seconds: 6
    finality_wait_seconds: 15
  base:
    enabled: true
    rpc_url: "https://base-mainnet.g.alchemy.com/v2/..."
    poll_interval_seconds: 3
    finality_wait_seconds: 10

topics:
  # DelegateVotesChanged
  0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724: "DelegateVotesChanged(address delegate, uint256 previousBalance, uint256 newBalance)"
  # Transfer
  0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef: "Transfer(address from, address to, uint256 value)"
```

* **chains**: Per-network RPC & timing settings.
* **topics**: Only `DelegateVotesChanged` and `Transfer` signatures are handled by built‑in modules; other topics can be added but require custom handlers.

---

## 4. API Module

### `POST /sync/historical`

* **Request Body**:

  ```json
  {
    "network": "ethereum",
    "tokenAddress": "0x...",
    "fromBlock": 12_000_000,
    "toBlock": 12_010_000
  }
  ```
* Enqueues a historical sync job.

### `GET /status/{jobId}`

* Returns job progress and errors.

---

## 5. Poller Module (Realtime)

1. Load last synced block for each `(network, tokenAddress)`.
2. Compute head = `rpc.block_number() - finality_wait_seconds`
3. For each token:

   ```rust
   let from = last_synced + 1;
   let to = (from + batch_size).min(head);
   enqueue_job(network, token, from, to);
   update_last_synced(network, token, to);
   ```

---

## 6. Worker / Processor Module

* **Job**: `{ network, tokenAddress, fromBlock, toBlock }`
* **Fetch Logs** with `eth_getLogs` using:

  * `address = tokenAddress`
  * `topics = [transfer_topic_hash, delegate_topic_hash]`
* **Deduplicate** raw logs by `(network, token, blockNumber, txIndex, logIndex, topicHash)`
* **Persist** into `rawEvents`
* **Dispatch** to handlers:

  * `TransferHandler.handle(Log)`
  * `DelegateHandler.handle(Log)`

---

## 7. Handler Implementation (Rust)

```rust
// 7.1 Trait definition
pub trait EventHandler {
    fn handle(&self, event: RawEventData, db: &MongoClient) -> Result<(), HandlerError>;
}

// 7.2 Transfer handler
pub struct TransferHandler;
impl EventHandler for TransferHandler {
    fn handle(&self, event: RawEventData, db: &MongoClient) -> Result<(), HandlerError> {
        let from = event.parse_address(0)?;
        let to = event.parse_address(1)?;
        let value = event.parse_uint256(2)?;

        // Insert memberTx for outgoing
        db.collection("memberTx").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &from, "direction": "outgoing", "blockNumber": event.block_number},
            doc! {"$setOnInsert": {/* fields */}},
            UpdateOptions::builder().upsert(true).build()
        )?;
        // Insert memberTx for incoming
        db.collection("memberTx").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &to, "direction": "incoming", "blockNumber": event.block_number},
            doc! {"$setOnInsert": {/* fields */}},
            UpdateOptions::builder().upsert(true).build()
        )?;

        // Recompute balances
        let from_balance = compute_balance(db, &event.token, &from)?;
        let to_balance = compute_balance(db, &event.token, &to)?;
        db.collection("memberBalances").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &from},
            doc! {"$set": {"balance": &from_balance, "updatedAt": bson::DateTime::now()}},
            UpdateOptions::builder().upsert(true).build()
        )?;
        db.collection("memberBalances").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &to},
            doc! {"$set": {"balance": &to_balance, "updatedAt": bson::DateTime::now()}},
            UpdateOptions::builder().upsert(true).build()
        )?;
        Ok(())
    }
}

// 7.3 Delegate handler
pub struct DelegateHandler;
impl EventHandler for DelegateHandler {
    fn handle(&self, event: RawEventData, db: &MongoClient) -> Result<(), HandlerError> {
        let delegate = event.parse_address(0)?;
        let prev = event.parse_uint256(1)?;
        let next = event.parse_uint256(2)?;
        
        db.collection("memberTx").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &delegate, "direction": "delegate", "blockNumber": event.block_number},
            doc! {"$setOnInsert": {"previousBalance": &prev, "newBalance": &next}},
            UpdateOptions::builder().upsert(true).build()
        )?;
        db.collection("memberBalances").update_one(
            doc! {"network": &event.network, "token": &event.token, "memberAddress": &delegate},
            doc! {"$set": {"votingPower": &next, "updatedAt": bson::DateTime::now()}},
            UpdateOptions::builder().upsert(true).build()
        )?;
        Ok(())
    }
}
```

* **RawEventData**: parsed log payload with helpers like `parse_address` and `parse_uint256`.
* **compute\_balance**: helper to sum incoming/outgoing transfers up to current block.

---

## 8. Data Models (MongoDB)

See original spec for `configIndexers`, `rawEvents`, `memberTx`, and `memberBalances`. Upsert and unique-index constraints apply.

---

## 9. Error Handling, Restart & Recovery

Remain as previously defined: retries, backoffs, dead-letter queues, and on-start replay of missing ranges.

---

With this configuration and handler skeleton, your Rust indexer will correctly process only `Transfer` and `DelegateVotesChanged` events per the provided YAML, maintain state in MongoDB, and support both historical and realtime ingestion.

