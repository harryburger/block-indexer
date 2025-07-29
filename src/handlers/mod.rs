use crate::models::*;
use anyhow::Result;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Macro to convert Event to ethers RawLog and decode using EthEvent
#[macro_export]
macro_rules! decode_event {
    ($event:expr, $event_type:ty) => {{
        let topics: Vec<ethers::types::H256> = $event
            .topics
            .iter()
            .map(|t| std::str::FromStr::from_str(t))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Invalid topic format: {}", e))?;

        let data = hex::decode($event.data.strip_prefix("0x").unwrap_or(&$event.data))
            .map_err(|e| anyhow::anyhow!("Invalid hex data: {}", e))?;

        let raw_log = ethers::abi::RawLog {
            topics,
            data,
        };

        <$event_type as ethers::contract::EthEvent>::decode_log(&raw_log)
            .map_err(|e| anyhow::anyhow!("Failed to decode event: {}", e))
    }};
}

pub use decode_event;

// Import individual handlers
pub mod gov_handler; // Shared functionality
pub mod gov_token_delegate;
pub mod gov_token_transfer;

use gov_token_delegate::GovTokenDelegateHandler;
use gov_token_transfer::GovTokenTransferHandler;

/// Trait that all event handlers must implement
#[async_trait::async_trait]
pub trait EventHandler: Send + Sync {
    /// Handle a batch of events of the same type
    async fn handle_batch(&self, events: Vec<Event>, registry: &HandlerRegistry) -> Result<()>;

    /// Handle a single event (default implementation calls handle_batch with single item)
    async fn handle(&self, event: Event, registry: &HandlerRegistry) -> Result<()> {
        self.handle_batch(vec![event], registry).await
    }
}

/// Registry for all available event handlers
pub struct HandlerRegistry {
    handlers: HashMap<String, Box<dyn EventHandler>>,
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerRegistry {
    /// Create a new handler registry with all available handlers
    pub fn new() -> Self {
        let mut handlers: HashMap<String, Box<dyn EventHandler>> = HashMap::new();

        // Register GovToken Transfer handler
        handlers.insert(
            "Transfer".to_string(),
            Box::new(GovTokenTransferHandler::new()),
        );

        // Register GovToken Delegate handler
        handlers.insert(
            "DelegateVotesChanged".to_string(),
            Box::new(GovTokenDelegateHandler::new()),
        );

        info!(
            "Handler registry initialized with {} handlers",
            handlers.len()
        );

        Self { handlers }
    }

    /// Get event signature from topic hash
    fn get_event_signature(&self, topic_hash: &str) -> Option<&str> {
        match topic_hash {
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" => {
                Some("Transfer")
            }
            "0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724" => {
                Some("DelegateVotesChanged")
            }
            _ => None,
        }
    }

    /// Process a batch of events, grouping them by topic and calling appropriate handlers
    pub async fn handle_events_batch(&self, events: Vec<Event>) -> Result<()> {
        info!(
            "🔄 Processing batch of {} events across all handlers",
            events.len()
        );

        // Group events by event signature
        let mut events_by_signature: HashMap<String, Vec<Event>> = HashMap::new();

        for event in events {
            // Use the stored event_signature if available, otherwise fallback to topic mapping
            let signature = event
                .event_signature
                .clone()
                .or_else(|| {
                    self.get_event_signature(&event.topic_hash)
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| format!("UNKNOWN_{}", &event.topic_hash[..10]));

            events_by_signature
                .entry(signature.clone())
                .or_default()
                .push(event);

            debug!("📝 Grouped event with signature: {}", signature);
        }

        info!("📊 Event distribution by signature:");

        for (signature, event_batch) in events_by_signature {
            let handler_key = if let Some(paren_pos) = signature.find('(') {
                signature[..paren_pos].to_string()
            } else {
                signature.clone()
            };

            if let Some(handler) = self.handlers.get(&handler_key) {
                info!(
                    "🚀 Processing {} {} events with dedicated handler",
                    event_batch.len(),
                    signature
                );

                match handler.handle_batch(event_batch, self).await {
                    Ok(_) => {
                        info!("✅ Successfully processed {} events", signature);
                    }
                    Err(e) => {
                        error!("❌ Failed to process {} events: {}", signature, e);
                        return Err(e);
                    }
                }
            } else {
                warn!(
                    "⚠️ No handler registered for event: {} (handler key: {}) (skipping {} events)",
                    signature,
                    handler_key,
                    event_batch.len()
                );
            }
        }

        info!("✅ Completed processing all event batches");
        Ok(())
    }

    /// Process a single event
    pub async fn handle_event(&self, event: Event) -> Result<()> {
        self.handle_events_batch(vec![event]).await
    }
}
