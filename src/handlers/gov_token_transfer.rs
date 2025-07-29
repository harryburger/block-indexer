use crate::handlers::gov_handler::GovHandler;
use crate::handlers::{EventHandler, HandlerRegistry};
use crate::models::*;
use crate::decode_event;
use anyhow::Result;
use bson::DateTime as BsonDateTime;
use ethers::contract::EthEvent;
use ethers::types::{Address, U256};
use ethers::utils::to_checksum;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::{info, warn};

#[derive(Clone, Debug, EthEvent)]
#[ethevent(
    name = "Transfer",
    abi = "Transfer(address indexed from, address indexed to, uint256 value)"
)]
pub struct TransferEvent {
    #[ethevent(indexed)]
    pub from: Address,
    #[ethevent(indexed)]
    pub to: Address,
    pub value: U256,
}

/// Handler for GovToken Transfer events
pub struct GovTokenTransferHandler;

impl Default for GovTokenTransferHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl GovTokenTransferHandler {
    pub fn new() -> Self {
        Self
    }

    /// Parse transfer event using EthEvent derive macro
    fn parse_transfer_event(event: &Event) -> Result<TransferEvent> {
        decode_event!(event, TransferEvent)
    }

    /// Create a transfer transaction record
    fn create_transfer_transaction(
        event: &Event,
        member_address: String,
        direction: TxDirection,
        value: String,
        current_time: BsonDateTime,
    ) -> MemberTx {
        let direction_str = match direction {
            TxDirection::Incoming => "incoming",
            TxDirection::Outgoing => "outgoing",
        };

        let unique_id = format!(
            "{}-{}-{}-{}-{}-{}-{}",
            event.tx_hash,
            event.network,
            event.address,
            member_address,
            event.tx_index,
            event.log_index,
            direction_str
        );

        MemberTx {
            id: None,
            unique_id: Some(unique_id),
            network: event.network.clone(),
            token: event.address.clone(),
            member_address,
            direction,
            tx_type: TxType::Transfer,
            block_number: event.block_number,
            tx_hash: event.tx_hash.clone(),
            value: Some(value),
            previous_balance: None,
            new_balance: None,
            created_at: current_time,
        }
    }

    /// Process events for a single token - SIMPLIFIED with shared code
    async fn process_token_events(token_address: String, events: Vec<Event>) -> Result<()> {
        let current_time = BsonDateTime::now();
        let network = events[0].network.clone();

        info!(
            "🔄 Processing {} transfer events for token {}",
            events.len(),
            token_address
        );

        let mut member_txs = Vec::new();
        let mut affected_members = HashSet::new();

        // Parse events - ONLY custom logic
        for event in &events {
            match Self::parse_transfer_event(event) {
                Ok(transfer_event) => {
                    let value = transfer_event.value.to_string();

                    // Handle outgoing transaction (from)
                    if transfer_event.from != Address::zero() {
                        let from_addr = to_checksum(&transfer_event.from, None);
                        member_txs.push(Self::create_transfer_transaction(
                            event,
                            from_addr.clone(),
                            TxDirection::Outgoing,
                            value.clone(),
                            current_time,
                        ));
                        affected_members.insert(from_addr);
                    }

                    // Handle incoming transaction (to)
                    if transfer_event.to != Address::zero() {
                        let to_addr = to_checksum(&transfer_event.to, None);
                        member_txs.push(Self::create_transfer_transaction(
                            event,
                            to_addr.clone(),
                            TxDirection::Incoming,
                            value.clone(),
                            current_time,
                        ));
                        affected_members.insert(to_addr);
                    }
                }
                Err(e) => {
                    warn!("Failed to decode transfer event: {}", e);
                    continue;
                }
            }
        }

        // Fetch balances from chain and create balance updates
        let members_list: Vec<String> = affected_members.into_iter().collect();
        let token_parsed = Address::from_str(&token_address).unwrap_or_default();

        info!(
            "🔗 Fetching balances for {} unique members for token {}",
            members_list.len(),
            token_address
        );

        // Fetch balances from chain at the block number when events occurred
        let event_block_number = events.first().map(|e| e.block_number);
        let balance_map = GovHandler::fetch_member_balances(
            &network,
            &token_parsed,
            &members_list,
            event_block_number,
        )
        .await;

        // Create balance updates using SHARED function
        let balance_updates = GovHandler::create_member_balance_records(
            &network,
            &token_address,
            &members_list,
            &balance_map,
            current_time,
        );

        // Save everything using SHARED function
        GovHandler::save_transactions_and_balances(
            &member_txs,
            &balance_updates,
            "transfer",
            &token_address,
        )
        .await?;

        info!(
            "✅ Completed processing transfer events for token {}",
            token_address
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl EventHandler for GovTokenTransferHandler {
    /// Main handler entry point - uses shared parallel processing logic
    async fn handle_batch(&self, events: Vec<Event>, _registry: &HandlerRegistry) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("🔄 Processing {} Transfer events", events.len());

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

        info!("✅ Successfully processed all Transfer events");
        Ok(())
    }
}
