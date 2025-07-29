use crate::handlers::gov_handler::GovHandler;
use crate::handlers::{EventHandler, HandlerRegistry};
use crate::models::*;
use crate::decode_event;
use anyhow::{anyhow, Result};
use bson::DateTime as BsonDateTime;
use ethers::contract::EthEvent;
use ethers::types::{Address, U256};
use ethers::utils::to_checksum;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

#[derive(Clone, Debug, EthEvent)]
#[ethevent(
    name = "DelegateVotesChanged",
    abi = "DelegateVotesChanged(address indexed delegate, uint256 previousBalance, uint256 newBalance)"
)]
pub struct DelegateVotesChangedEvent {
    #[ethevent(indexed)]
    pub delegate: Address,
    pub previous_balance: U256,
    pub new_balance: U256,
}

/// Handler for GovToken DelegateVotesChanged events
pub struct GovTokenDelegateHandler;

impl Default for GovTokenDelegateHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl GovTokenDelegateHandler {
    pub fn new() -> Self {
        Self
    }

    /// Decode DelegateVotesChanged event from raw event data
    fn decode_delegate_event(event: &Event) -> Result<DelegateVotesChangedEvent> {
        let parsed = decode_event!(event, DelegateVotesChangedEvent)?;

        // Check for zero address
        if parsed.delegate == Address::zero() {
            return Err(anyhow!("Zero address delegate"));
        }

        Ok(parsed)
    }

    /// Create a delegate transaction record
    fn create_delegate_transaction(
        event: &Event,
        delegate_event: &DelegateVotesChangedEvent,
        current_time: BsonDateTime,
    ) -> MemberTx {
        let delegate_address = to_checksum(&delegate_event.delegate, None);
        let unique_id = format!(
            "{}-{}-{}-{}-{}-{}-delegate",
            event.tx_hash,
            event.network,
            event.address,
            delegate_address,
            event.tx_index,
            event.log_index
        );

        MemberTx {
            id: None,
            unique_id: Some(unique_id),
            network: event.network.clone(),
            token: event.address.clone(),
            member_address: delegate_address,
            direction: TxDirection::Incoming,
            tx_type: TxType::Delegate,
            block_number: event.block_number,
            tx_hash: event.tx_hash.clone(),
            value: None,
            previous_balance: Some(delegate_event.previous_balance.to_string()),
            new_balance: Some(delegate_event.new_balance.to_string()),
            created_at: current_time,
        }
    }

    /// Process events for a single token - SIMPLIFIED with shared code
    async fn process_token_events(token_address: String, events: Vec<Event>) -> Result<()> {
        let current_time = BsonDateTime::now();
        let network = events[0].network.clone();

        info!(
            "🔄 Processing {} delegate events for token {}",
            events.len(),
            token_address
        );

        let mut member_txs = Vec::new();
        let mut affected_members = HashSet::new();
        let mut voting_powers = HashMap::new();

        // Parse events - ONLY custom logic
        for event in &events {
            match Self::decode_delegate_event(event) {
                Ok(delegate_event) => {
                    let delegate_address = to_checksum(&delegate_event.delegate, None);

                    member_txs.push(Self::create_delegate_transaction(
                        event,
                        &delegate_event,
                        current_time,
                    ));

                    affected_members.insert(delegate_address.clone());
                    voting_powers.insert(delegate_address, delegate_event.new_balance.to_string());
                }
                Err(e) => {
                    warn!("Failed to decode delegate event: {}", e);
                    continue;
                }
            }
        }

        // Create balance updates using SHARED function - NO CHAIN CALLS for delegate events
        let balance_updates = GovHandler::create_delegate_balance_records(
            &network,
            &token_address,
            &voting_powers, // Use voting power directly from events
            current_time,
        );

        // Save everything using SHARED function
        GovHandler::save_transactions_and_balances(
            &member_txs,
            &balance_updates,
            "delegate",
            &token_address,
        )
        .await?;

        info!(
            "✅ Completed processing delegate events for token {}",
            token_address
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl EventHandler for GovTokenDelegateHandler {
    /// Main handler entry point - uses shared parallel processing logic
    async fn handle_batch(&self, events: Vec<Event>, _registry: &HandlerRegistry) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("🔄 Processing {} DelegateVotesChanged events", events.len());

        // Group events by token address
        let mut events_by_token: HashMap<String, Vec<Event>> = HashMap::new();
        for event in events {
            events_by_token
                .entry(event.address.clone())
                .or_default()
                .push(event);
        }

        info!(
            "📊 Processing {} unique tokens in parallel",
            events_by_token.len()
        );

        // Process each token's events in parallel
        let mut handles = Vec::new();
        for (token_address, token_events) in events_by_token {
            let handle = tokio::spawn(async move {
                Self::process_token_events(token_address, token_events).await
            });
            handles.push(handle);
        }

        // Wait for all token processing to complete
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => warn!("Token processing failed: {}", e),
                Err(e) => warn!("Task join error: {}", e),
            }
        }

        info!("✅ Successfully processed all DelegateVotesChanged events");
        Ok(())
    }
}
