use chrono::{DateTime, Utc};
// Removed unused serde imports
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::chain_client::ChainClient;
use crate::config::AppConfig;
// Removed unused CollectionName import
use crate::database::DatabaseClient;
use crate::error::{DatabaseError, TokenSyncError};
use crate::handlers::HandlerRegistry;
use crate::models::{EthLog, Event};

// ProcessedEvent removed - using Event from models instead

pub struct LogProcessor {
    config: AppConfig,
    database: std::sync::Arc<DatabaseClient>,
    source_db: Option<crate::source::MongoClient>,
    watched_tokens: std::sync::Arc<RwLock<HashMap<String, HashSet<String>>>>,
    handler_registry: HandlerRegistry,
    chain_clients: std::sync::Arc<HashMap<String, std::sync::Arc<tokio::sync::Mutex<ChainClient>>>>,
}

impl LogProcessor {
    pub async fn new(
        config: AppConfig,
        database: std::sync::Arc<DatabaseClient>,
        source_db: Option<crate::source::MongoClient>,
        chain_clients: std::sync::Arc<
            HashMap<String, std::sync::Arc<tokio::sync::Mutex<ChainClient>>>,
        >,
    ) -> Self {
        info!("✅ Log processor initialized");
        let mut handler_registry = HandlerRegistry::new();

        // Convert chain clients to the format expected by HandlerRegistry
        let chain_clients_for_handlers: HashMap<String, ChainClient> = {
            let mut converted = HashMap::new();
            for (name, client_arc) in chain_clients.iter() {
                if let Ok(client) = client_arc.try_lock() {
                    converted.insert(name.clone(), client.clone());
                }
            }
            converted
        };

        // Set chain clients on handler registry
        handler_registry.set_chain_clients(chain_clients_for_handlers);

        let processor = Self {
            config,
            database,
            source_db,
            watched_tokens: std::sync::Arc::new(RwLock::new(HashMap::new())),
            handler_registry,
            chain_clients,
        };

        // Load tokens from plugins synchronously before returning
        processor.load_watched_tokens_from_plugins().await;

        processor
    }

    /// Process ALL logs and filter based on cached tokens
    pub async fn process_logs(
        &self,
        network: &str,
        block_number: u64,
        _block_timestamp: u64,
        _block_hash: &str,
        logs: &[EthLog],
    ) -> Result<(), TokenSyncError> {
        let now = Utc::now();

        debug!(
            "🔍 Processing {} logs for block #{}",
            logs.len(),
            block_number
        );

        // Get cached tokens for this network
        let watched_tokens = match self.watched_tokens.try_read() {
            Ok(cache) => cache.get(network).cloned().unwrap_or_default(),
            Err(_) => {
                warn!("Failed to acquire read lock on watched_tokens cache");
                return Ok(());
            }
        };

        // Define token-specific topics (Transfer and DelegateVotesChanged)
        let token_topics = [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer
            "0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724", // DelegateVotesChanged
        ];

        // Get all configured topics for non-token events
        let all_configured_topics: HashSet<&str> =
            self.config.topics.keys().map(|s| s.as_str()).collect();

        let mut filtered_logs = Vec::new();

        for log in logs {
            let topic_hash = log.topics.first().map(|s| s.as_str()).unwrap_or("");

            // Check if this is a token event (Transfer or DelegateVotesChanged)
            if token_topics.contains(&topic_hash) {
                // For token events, only process if token is watched
                if watched_tokens.contains(&log.address) {
                    filtered_logs.push(log.clone());
                }
            } else {
                // For other events, process if topic is configured (regardless of token address)
                if all_configured_topics.contains(topic_hash) {
                    filtered_logs.push(log.clone());
                }
            }
        }

        // Save ONLY filtered raw logs (after filtering)
        self.save_raw_logs(network, block_number, &filtered_logs, now)
            .await?;

        // After saving raw logs, call handlers to process the events
        if !filtered_logs.is_empty() {
            info!(
                "🚀 Calling handlers for {} filtered events in block #{}",
                filtered_logs.len(),
                block_number
            );

            // Convert filtered logs to Events and get them from database
            // (the save_raw_logs method already saved them, so we get the saved events)
            let saved_events = self.get_events_for_block(network, block_number).await?;

            if !saved_events.is_empty() {
                match self
                    .handler_registry
                    .handle_events_batch(saved_events, &self.database)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "✅ Successfully processed handlers for block #{}",
                            block_number
                        );
                    }
                    Err(e) => {
                        error!(
                            "❌ Handler processing failed for block #{}: {}",
                            block_number, e
                        );
                        // Continue processing - handler failures shouldn't stop the sync
                    }
                }
            }
        }

        let token_count = filtered_logs
            .iter()
            .filter(|log| {
                let topic_hash = log.topics.first().map(|s| s.as_str()).unwrap_or("");
                token_topics.contains(&topic_hash)
            })
            .count();

        let other_count = filtered_logs.len() - token_count;

        info!(
            "✅ Block #{}: Filtered {} logs from {} total, saved {} events ({} token + {} other)",
            block_number,
            filtered_logs.len(),
            logs.len(),
            filtered_logs.len(),
            token_count,
            other_count
        );

        Ok(())
    }

    /// Load watched tokens from plugins collection
    async fn load_watched_tokens_from_plugins(&self) {
        info!("🔄 Loading watched tokens from plugins...");

        if let Some(source_db) = &self.source_db {
            for (network_name, chain_config) in &self.config.chains {
                if !chain_config.enabled {
                    continue;
                }

                match source_db.query_plugin_tokens(network_name).await {
                    Ok(token_addresses) => {
                        let token_set: HashSet<String> = token_addresses.into_iter().collect();

                        let mut cache = self.watched_tokens.write().await;
                        cache.insert(network_name.clone(), token_set);

                        info!(
                            "✅ Loaded {} watched tokens for {} from plugins",
                            cache.get(network_name).map(|s| s.len()).unwrap_or(0),
                            network_name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to load token addresses from plugins for {}: {}",
                            network_name, e
                        );
                    }
                }
            }
        } else {
            warn!("Source database not configured - cannot load tokens from plugins");
        }
    }

    /// Get saved events for a specific block to pass to handlers
    async fn get_events_for_block(
        &self,
        network: &str,
        block_number: u64,
    ) -> Result<Vec<Event>, TokenSyncError> {
        use futures::TryStreamExt;
        use mongodb::bson::doc;

        let collection = self.database.events();

        let filter = doc! {
            "network": network,
            "blockNumber": block_number as i64
        };

        match collection.find(filter).await {
            Ok(mut cursor) => {
                let mut events = Vec::new();

                while let Ok(Some(event)) = cursor.try_next().await {
                    events.push(event);
                }

                debug!(
                    "Retrieved {} events for block #{} to pass to handlers",
                    events.len(),
                    block_number
                );
                Ok(events)
            }
            Err(e) => {
                error!(
                    "Failed to retrieve events for block #{}: {}",
                    block_number, e
                );
                Err(DatabaseError::QueryFailed(format!("Failed to retrieve events: {}", e)).into())
            }
        }
    }

    /// Get human-readable event signature from topic hash
    fn get_event_signature(&self, topic_hash: &str) -> Option<String> {
        // First check the config topics
        if let Some(signature) = self.config.topics.get(topic_hash) {
            return Some(signature.clone());
        }

        // Fallback to hardcoded common event signatures
        match topic_hash {
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => {
                Some("Transfer(address,address,uint256)".to_string())
            }
            "0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724" => {
                Some("DelegateVotesChanged(address,uint256,uint256)".to_string())
            }
            "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" => {
                Some("Approval(address,address,uint256)".to_string())
            }
            _ => None,
        }
    }

    /// Save raw logs using bulk operations
    async fn save_raw_logs(
        &self,
        network: &str,
        block_number: u64,
        logs: &[EthLog],
        created_at: DateTime<Utc>,
    ) -> Result<(), TokenSyncError> {
        if logs.is_empty() {
            return Ok(());
        }

        info!(
            "🔍 Converting {} logs to events for network: {}, block: {}",
            logs.len(),
            network,
            block_number
        );

        let events: Vec<Event> = logs
            .iter()
            .enumerate()
            .filter_map(|(i, log)| {
                // Note: Basic validation is now done at the poller level,
                // but we still validate hex parsing here for safety
                let topic_hash = log.topics.first().cloned().unwrap_or_default();
                let event_signature = self.get_event_signature(&topic_hash);

                let tx_index = if log.transaction_index.len() > 2 {
                    match u32::from_str_radix(&log.transaction_index[2..], 16) {
                        Ok(val) => val,
                        Err(e) => {
                            warn!(
                                "⚠️ Skipping log {}/{}: invalid transaction_index format '{}': {}",
                                i + 1,
                                logs.len(),
                                log.transaction_index,
                                e
                            );
                            return None;
                        }
                    }
                } else {
                    warn!(
                        "⚠️ Skipping log {}/{}: transaction_index too short: '{}'",
                        i + 1,
                        logs.len(),
                        log.transaction_index
                    );
                    return None;
                };

                let log_index = if log.log_index.len() > 2 {
                    match u32::from_str_radix(&log.log_index[2..], 16) {
                        Ok(val) => val,
                        Err(e) => {
                            warn!(
                                "⚠️ Skipping log {}/{}: invalid log_index format '{}': {}",
                                i + 1,
                                logs.len(),
                                log.log_index,
                                e
                            );
                            return None;
                        }
                    }
                } else {
                    warn!(
                        "⚠️ Skipping log {}/{}: log_index too short: '{}'",
                        i + 1,
                        logs.len(),
                        log.log_index
                    );
                    return None;
                };

                let event = Event {
                    id: None,
                    network: network.to_string(),
                    block_number,
                    tx_hash: log.transaction_hash.clone(),
                    tx_index,
                    log_index,
                    topic_hash: topic_hash.clone(),
                    address: log.address.clone(),
                    data: log.data.clone(),
                    topics: log.topics.clone(),
                    event_signature,
                    created_at: bson::DateTime::from_chrono(created_at),
                };

                Some(event)
            })
            .collect();

        if events.is_empty() {
            warn!("⚠️ No valid events to save after filtering");
            return Ok(());
        }

        info!(
            "💾 Attempting to insert {} valid events into database",
            events.len()
        );

        match self
            .database
            .events()
            .insert_many(events)
            .with_options(
                mongodb::options::InsertManyOptions::builder()
                    .ordered(false)
                    .build(),
            )
            .await
        {
            Ok(result) => {
                info!(
                    "✅ Successfully inserted {} events",
                    result.inserted_ids.len()
                );
            }
            Err(e) => {
                if e.to_string().contains("duplicate key") || e.to_string().contains("E11000") {
                    info!("⚠️ Some events already exist (duplicates skipped)");
                } else {
                    error!("❌ Failed to save events: {}", e);
                    return Err(
                        DatabaseError::InsertFailed(format!("Failed to save events: {e}")).into(),
                    );
                }
            }
        }

        Ok(())
    }

    /// Get chain client for a specific network
    pub async fn get_chain_client(&self, network: &str) -> Option<ChainClient> {
        match self.chain_clients.get(network) {
            Some(client_arc) => match client_arc.try_lock() {
                Ok(client) => Some(client.clone()),
                Err(_) => {
                    warn!("Failed to acquire lock on chain client for {}", network);
                    None
                }
            },
            None => {
                warn!("No chain client found for network: {}", network);
                None
            }
        }
    }

    // save_events removed - using save_raw_logs instead
}

impl Clone for LogProcessor {
    fn clone(&self) -> Self {
        let mut handler_registry = HandlerRegistry::new();

        // Convert chain clients for the cloned handler registry
        let chain_clients_for_handlers: HashMap<String, ChainClient> = {
            let mut converted = HashMap::new();
            for (name, client_arc) in self.chain_clients.iter() {
                if let Ok(client) = client_arc.try_lock() {
                    converted.insert(name.clone(), client.clone());
                }
            }
            converted
        };

        handler_registry.set_chain_clients(chain_clients_for_handlers);

        Self {
            config: self.config.clone(),
            database: self.database.clone(),
            source_db: self.source_db.clone(),
            watched_tokens: self.watched_tokens.clone(),
            handler_registry,
            chain_clients: self.chain_clients.clone(),
        }
    }
}
