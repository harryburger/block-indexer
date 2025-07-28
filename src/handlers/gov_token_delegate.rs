use crate::database::DatabaseClient;
use crate::handlers::{EventHandler, HandlerRegistry};
use crate::models::*;
use anyhow::Result;
use bson::DateTime as BsonDateTime;
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
    async fn handle_batch(&self, events: Vec<Event>, db: &DatabaseClient, registry: &HandlerRegistry) -> Result<()> {
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

        // Update balances with on-chain data
        let mut balance_updates = Vec::new();
        for (_key, (network, token, address, event_voting_power)) in balances_to_update {
            let (balance, voting_power) = match registry.get_chain_client(&network) {
                Some(chain_client) => {
                    debug!("🔗 Fetching on-chain data for {} on {} token {}", address, network, token);
                    
                    // Fetch both balance and voting power from chain
                    let balance_result = chain_client.get_token_balance(&token, &address).await;
                    let votes_result = chain_client.get_voting_power(&token, &address).await;
                    
                    let balance = match balance_result {
                        Ok(bal) => {
                            debug!("✅ Got balance {} for {} on {}", bal, address, token);
                            bal
                        }
                        Err(e) => {
                            warn!("Failed to fetch on-chain balance for {}: {}, using event data", address, e);
                            event_voting_power.clone()
                        }
                    };
                    
                    let voting_power = match votes_result {
                        Ok(votes) => {
                            debug!("✅ Got voting power {} for {} on {}", votes, address, token);
                            votes
                        }
                        Err(e) => {
                            warn!("Failed to fetch on-chain voting power for {}: {}, using event data", address, e);
                            event_voting_power.clone()
                        }
                    };
                    
                    (balance, voting_power)
                }
                None => {
                    warn!("No chain client for network {}, using event data and DB fallback", network);
                    
                    // Fallback to database computation for balance
                    let balance = match db.compute_balance(&network, &token, &address).await {
                        Ok(db_balance) => db_balance,
                        Err(e) => {
                            warn!("Failed to compute DB balance for {}: {}", address, e);
                            event_voting_power.clone()
                        }
                    };
                    
                    (balance, event_voting_power)
                }
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
