use crate::database::DatabaseClient;
use crate::handlers::EventHandler;
use crate::models::*;
use anyhow::Result;
use bson::DateTime as BsonDateTime;
use std::collections::HashMap;
use tracing::{info, warn};

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
}

#[async_trait::async_trait]
impl EventHandler for GovTokenDelegateHandler {
    async fn handle_batch(&self, events: Vec<Event>, db: &DatabaseClient) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("🔄 Processing {} DelegateVotesChanged events", events.len());

        let current_time = BsonDateTime::now();
        let mut member_txs = Vec::new();
        let mut balances_to_update = HashMap::new(); // address -> (network, token, address, new_balance)

        // Process each delegate event
        for event in events {
            // Parse delegate data
            let delegate = event.topics.get(1).cloned().unwrap_or_default();

            if delegate.is_empty() || delegate == "0x0000000000000000000000000000000000000000" {
                continue;
            }

            let delegate_addr = format!("0x{}", &delegate[26..66]); // Extract address from topic

            // Parse previous and new balances from data
            if event.data.len() >= 130 {
                // 0x + 128 hex chars (64 bytes)
                let prev_hex = &event.data[2..66];
                let new_hex = &event.data[66..130];

                let previous_balance = ethers::types::U256::from_str_radix(prev_hex, 16)
                    .map(|a| a.to_string())
                    .unwrap_or("0".to_string());

                let new_balance = ethers::types::U256::from_str_radix(new_hex, 16)
                    .map(|a| a.to_string())
                    .unwrap_or("0".to_string());

                // Create delegate transaction
                member_txs.push(MemberTx {
                    id: None,
                    unique_id: None,
                    network: event.network.clone(),
                    token: event.address.clone(), // Use event.address as token
                    member_address: delegate_addr.clone(),
                    direction: TxDirection::Delegate,
                    block_number: event.block_number,
                    tx_hash: event.tx_hash.clone(),
                    value: None,
                    previous_balance: Some(previous_balance),
                    new_balance: Some(new_balance.clone()),
                    created_at: current_time,
                });

                // Mark for balance update with voting power
                let key = format!("{}-{}-{}", event.network, event.address, delegate_addr);
                balances_to_update.insert(
                    key,
                    (
                        event.network.clone(),
                        event.address.clone(), // Use event.address as token
                        delegate_addr,
                        new_balance,
                    ),
                );
            }
        }

        // Save member transactions
        if !member_txs.is_empty() {
            let txs_len = member_txs.len();
            match db.member_tx().insert_many(member_txs).await {
                Ok(_) => info!("✅ Saved {} delegate transactions", txs_len),
                Err(e) => {
                    if e.to_string().contains("duplicate key") {
                        info!("⚠️ Some delegate transactions already exist (duplicates skipped)");
                    } else {
                        warn!("Failed to save delegate transactions: {}", e);
                    }
                }
            }
        }

        // Update balances with voting power
        let mut balance_updates = Vec::new();
        for (_key, (network, token, address, voting_power)) in balances_to_update {
            // Get current balance from database or compute it
            let balance = match db.compute_balance(&network, &token, &address).await {
                Ok(bal) => bal,
                Err(_) => voting_power.clone(), // Fallback to voting power as balance
            };

            balance_updates.push(MemberBalance {
                id: None,
                unique_id: Some(format!("{}-{}-{}", network, token, address)),
                network,
                token,
                member_address: address,
                balance,
                voting_power: Some(voting_power),
                updated_at: current_time,
            });
        }

        if !balance_updates.is_empty() {
            let balance_len = balance_updates.len();
            match db.member_balances().insert_many(balance_updates).await {
                Ok(_) => info!("✅ Updated {} delegate balances", balance_len),
                Err(e) => {
                    if e.to_string().contains("duplicate key") {
                        info!("⚠️ Some delegate balances already exist (duplicates skipped)");
                    } else {
                        warn!("Failed to update delegate balances: {}", e);
                    }
                }
            }
        }

        info!("✅ Successfully processed DelegateVotesChanged events");
        Ok(())
    }
}
