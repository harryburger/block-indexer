use chrono::Utc;
// Removed unused serde imports
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, warn};

use crate::app_state::{get_db_client, with_global_state};
use crate::chain_client::ChainClient;
use crate::config::AppConfig;
use crate::error::{DatabaseError, TokenSyncError};
use crate::handlers::HandlerRegistry;
use crate::models::Event;
use ethers::types::{Address, Log};
use ethers::utils::to_checksum;

// ProcessedEvent removed - using Event from models instead

pub struct LogProcessor {
    config: AppConfig,
    watched_tokens: HashMap<String, HashSet<String>>,
    handler_registry: HandlerRegistry,
    chain_clients: std::sync::Arc<HashMap<String, std::sync::Arc<tokio::sync::Mutex<ChainClient>>>>,
}

impl LogProcessor {
    pub async fn new(
        config: AppConfig,
        chain_clients: std::sync::Arc<
            HashMap<String, std::sync::Arc<tokio::sync::Mutex<ChainClient>>>,
        >,
    ) -> Self {
        info!("✅ Log processor initialized");

        // Create handler registry
        let handler_registry = HandlerRegistry::new();

        // Load watched tokens from plugins using global state
        let watched_tokens = Self::load_watched_tokens(&config).await;

        Self {
            config,
            watched_tokens,
            handler_registry,
            chain_clients,
        }
    }

    /// Load watched tokens from plugins as a simple HashMap
    async fn load_watched_tokens(config: &AppConfig) -> HashMap<String, HashSet<String>> {
        let source_db = with_global_state(|state| state.source_db().cloned()).flatten();
        let mut watched_tokens = HashMap::new();

        if let Some(source_db) = &source_db {
            info!("🔄 Loading watched tokens from plugins...");

            for (network_name, chain_config) in &config.chains {
                if !chain_config.enabled {
                    continue;
                }

                match source_db.query_plugin_tokens(network_name).await {
                    Ok(token_addresses) => {
                        let token_set: HashSet<String> = token_addresses.into_iter().collect();
                        info!(
                            "✅ Loaded {} watched tokens for {}",
                            token_set.len(),
                            network_name
                        );
                        debug!("📋 Watched tokens for {}: {:?}", network_name, token_set);
                        watched_tokens.insert(network_name.clone(), token_set);
                    }
                    Err(e) => {
                        warn!("Failed to load token addresses for {}: {}", network_name, e);
                    }
                }
            }
        } else {
            warn!("Source database not configured - no tokens loaded");
        }

        watched_tokens
    }

    /// Process logs and filter based on watched tokens
    pub async fn process_logs(
        &self,
        network: &str,
        block_number: u64,
        logs: &[Log],
    ) -> Result<(), TokenSyncError> {
        let now = Utc::now();

        debug!(
            "🔍 Processing {} logs for block #{}",
            logs.len(),
            block_number
        );

        // Get watched tokens for this network
        let watched_tokens = self
            .watched_tokens
            .get(network)
            .cloned()
            .unwrap_or_default();

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
            let Some(first_topic) = log.topics.first() else {
                continue;
            };

            let topic_hash = format!("0x{:064x}", first_topic);

            if token_topics.contains(&topic_hash.as_str()) {
                let checksummed_address = Self::to_checksummed_address(&log.address);
                if watched_tokens.contains(&checksummed_address) {
                    filtered_logs.push(log.clone());
                }
            } else if all_configured_topics.contains(topic_hash.as_str()) {
                filtered_logs.push(log.clone());
            }
        }

        // Convert filtered logs to Events for both saving and handler processing
        let events: Vec<Event> = filtered_logs
            .iter()
            .enumerate()
            .filter_map(|(i, log)| {
                // Validate required fields are present
                let transaction_hash = log.transaction_hash?;
                let transaction_index = log.transaction_index?;
                let log_index = log.log_index?;

                // Check for zero/invalid values
                if transaction_hash.is_zero() || log.topics.is_empty() || log.address.is_zero() {
                    warn!(
                        "⚠️ Skipping invalid log {}/{} in block #{}",
                        i + 1,
                        filtered_logs.len(),
                        block_number
                    );
                    return None;
                }

                // Convert to strings once
                let tx_hash_str = format!("0x{:064x}", transaction_hash);
                let address_str = Self::to_checksummed_address(&log.address); // Use checksummed format
                let topic_hash = format!("0x{:064x}", log.topics[0]);
                let data_str = format!("0x{}", hex::encode(&log.data));
                let topics_str: Vec<String> = log
                    .topics
                    .iter()
                    .map(|topic| format!("0x{:064x}", topic))
                    .collect();

                let tx_index = transaction_index.as_u32();
                let log_idx = log_index.as_u32();

                let unique_id = Self::generate_unique_id(
                    &tx_hash_str,
                    network,
                    tx_index,
                    log_idx,
                    &address_str,
                );
                let event_signature = self.get_event_signature(&topic_hash);

                Some(Event {
                    id: None,
                    unique_id,
                    network: network.to_string(),
                    block_number,
                    tx_hash: tx_hash_str,
                    tx_index: tx_index,
                    log_index: log_idx,
                    topic_hash,
                    address: address_str,
                    data: data_str,
                    topics: topics_str,
                    event_signature,
                    created_at: bson::DateTime::from_chrono(now),
                })
            })
            .collect();

        let events_count = events.len();

        if !events.is_empty() {
            self.save_events(&events).await?;
        }

        if !events.is_empty() {
            info!(
                "🚀 Calling handlers for {} filtered events in block #{}",
                events_count, block_number
            );

            match self.handler_registry.handle_events_batch(events).await {
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
                }
            }
        }

        info!(
            "✅ Block #{}: Processed {} events from {} total logs",
            block_number,
            events_count,
            logs.len()
        );

        Ok(())
    }

    /// Convert address to checksummed format for comparison
    fn to_checksummed_address(address: &Address) -> String {
        to_checksum(address, None)
    }

    /// Generate unique ID for an event: transactionHash-network-txIndex-logIndex-address
    fn generate_unique_id(
        tx_hash: &str,
        network: &str,
        tx_index: u32,
        log_index: u32,
        address: &str,
    ) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            tx_hash, network, tx_index, log_index, address
        )
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

    /// Save events using bulk operations
    async fn save_events(&self, events: &[Event]) -> Result<(), TokenSyncError> {
        if events.is_empty() {
            return Ok(());
        }

        info!(
            "💾 Attempting to insert {} events into database",
            events.len()
        );

        let database = get_db_client().ok_or_else(|| {
            TokenSyncError::DatabaseError(DatabaseError::ConnectionFailed(
                "Global state not initialized".to_string(),
            ))
        })?;

        match database
            .events()
            .insert_many(events.to_vec())
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

    /// Get chain client for a specific network (for handlers)
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
}
