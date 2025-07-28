use crate::database::DatabaseClient;
use crate::handlers::{EventHandler, HandlerRegistry};
use crate::models::*;
use anyhow::Result;
use bson::DateTime as BsonDateTime;
use std::collections::HashMap;
use tracing::{debug, info, warn};

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
}

#[async_trait::async_trait]
impl EventHandler for GovTokenTransferHandler {
    async fn handle_batch(&self, events: Vec<Event>, db: &DatabaseClient, registry: &HandlerRegistry) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("🔄 Processing {} Transfer events", events.len());

        let current_time = BsonDateTime::now();
        let mut member_txs = Vec::new();
        let mut balances_to_update = HashMap::new(); // network-token-address -> address info

        // Process each event and create transactions
        for event in events {
            // Parse transfer data
            let from = event.topics.get(1).cloned().unwrap_or_default();
            let to = event.topics.get(2).cloned().unwrap_or_default();
            let value = if event.data.len() > 66 {
                let amount_hex = &event.data[2..66];
                ethers::types::U256::from_str_radix(amount_hex, 16)
                    .map(|a| a.to_string())
                    .unwrap_or("0".to_string())
            } else {
                "0".to_string()
            };

            // Create outgoing transaction (from)
            if !from.is_empty() && from != "0x0000000000000000000000000000000000000000" {
                let from_addr = format!("0x{}", &from[26..66]); // Extract address from topic

                member_txs.push(MemberTx {
                    id: None,
                    unique_id: None,
                    network: event.network.clone(),
                    token: event.address.clone(), // Use event.address as token
                    member_address: from_addr.clone(),
                    direction: TxDirection::Outgoing,
                    block_number: event.block_number,
                    tx_hash: event.tx_hash.clone(),
                    value: Some(value.clone()),
                    previous_balance: None,
                    new_balance: None,
                    created_at: current_time,
                });

                // Mark for balance update
                let key = format!("{}-{}-{}", event.network, event.address, from_addr);
                balances_to_update.insert(
                    key,
                    (event.network.clone(), event.address.clone(), from_addr),
                );
            }

            // Create incoming transaction (to)
            if !to.is_empty() && to != "0x0000000000000000000000000000000000000000" {
                let to_addr = format!("0x{}", &to[26..66]); // Extract address from topic

                member_txs.push(MemberTx {
                    id: None,
                    unique_id: None,
                    network: event.network.clone(),
                    token: event.address.clone(), // Use event.address as token
                    member_address: to_addr.clone(),
                    direction: TxDirection::Incoming,
                    block_number: event.block_number,
                    tx_hash: event.tx_hash.clone(),
                    value: Some(value.clone()),
                    previous_balance: None,
                    new_balance: None,
                    created_at: current_time,
                });

                // Mark for balance update
                let key = format!("{}-{}-{}", event.network, event.address, to_addr);
                balances_to_update
                    .insert(key, (event.network.clone(), event.address.clone(), to_addr));
            }
        }

        // Save member transactions
        if !member_txs.is_empty() {
            let txs_len = member_txs.len();
            match db.member_tx().insert_many(member_txs).await {
                Ok(_) => info!("✅ Saved {} member transactions", txs_len),
                Err(e) => {
                    if e.to_string().contains("duplicate key") {
                        info!("⚠️ Some transactions already exist (duplicates skipped)");
                    } else {
                        warn!("Failed to save member transactions: {}", e);
                    }
                }
            }
        }

        // Update balances using chain client for real-time data
        let mut balance_updates = Vec::new();
        for (_key, (network, token, address)) in balances_to_update {
            let balance = match registry.get_chain_client(&network) {
                Some(chain_client) => {
                    debug!("🔗 Fetching on-chain balance for {} on {} token {}", address, network, token);
                    match chain_client.get_token_balance(&token, &address).await {
                        Ok(balance) => {
                            debug!("✅ Got balance {} for {} on {}", balance, address, token);
                            balance
                        }
                        Err(e) => {
                            warn!("Failed to fetch on-chain balance for {}: {}, falling back to DB", address, e);
                            // Fallback to database computation
                            match db.compute_balance(&network, &token, &address).await {
                                Ok(db_balance) => db_balance,
                                Err(e2) => {
                                    warn!("Failed to compute DB balance for {}: {}", address, e2);
                                    "0".to_string()
                                }
                            }
                        }
                    }
                }
                None => {
                    warn!("No chain client for network {}, using DB balance", network);
                    // Fallback to database computation
                    match db.compute_balance(&network, &token, &address).await {
                        Ok(db_balance) => db_balance,
                        Err(e) => {
                            warn!("Failed to compute DB balance for {}: {}", address, e);
                            "0".to_string()
                        }
                    }
                }
            };

            balance_updates.push(MemberBalance {
                id: None,
                unique_id: Some(format!("{}-{}-{}", network, token, address)),
                network,
                token,
                member_address: address,
                balance,
                voting_power: None, // Will be updated by delegate handler
                updated_at: current_time,
            });
        }

        if !balance_updates.is_empty() {
            let balance_len = balance_updates.len();
            match db.member_balances().insert_many(balance_updates).await {
                Ok(_) => info!("✅ Updated {} member balances", balance_len),
                Err(e) => {
                    if e.to_string().contains("duplicate key") {
                        info!("⚠️ Some balances already exist (duplicates skipped)");
                    } else {
                        warn!("Failed to update member balances: {}", e);
                    }
                }
            }
        }

        info!("✅ Successfully processed Transfer events");
        Ok(())
    }
}
