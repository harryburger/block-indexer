use crate::app_state::{get_chain_client, get_db_client};
use crate::models::*;
use anyhow::Result;
use bson::{doc, DateTime as BsonDateTime};
use ethers::types::Address;
use futures::stream::TryStreamExt;
use mongodb::{
    options::{InsertOneModel, UpdateOneModel, WriteModel},
    results::SummaryBulkWriteResult,
    Namespace,
};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::{info, warn};

/// Common constants for governance token handlers
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Shared governance token handler functionality
pub struct GovHandler;

impl GovHandler {
    /// Save member transactions in bulk, avoiding duplicates
    pub async fn save_member_transactions_bulk(
        member_txs: &[MemberTx],
    ) -> Result<SummaryBulkWriteResult> {
        if member_txs.is_empty() {
            return Ok(Default::default());
        }

        let db = get_db_client().ok_or_else(|| anyhow::anyhow!("Global state not initialized"))?;

        // Check for existing transactions to avoid duplicates
        let unique_ids: Vec<String> = member_txs
            .iter()
            .filter_map(|tx| tx.unique_id.clone())
            .collect();

        let existing_ids: HashSet<String> = db
            .member_tx()
            .find(doc! { "uniqueId": {"$in": &unique_ids} })
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|doc| doc.unique_id)
            .collect();

        // Filter out duplicates
        let new_transactions: Vec<_> = member_txs
            .iter()
            .filter(|tx| {
                tx.unique_id
                    .as_ref()
                    .map(|id| !existing_ids.contains(id))
                    .unwrap_or(true)
            })
            .collect();

        if new_transactions.is_empty() {
            info!("All transactions already exist, skipping duplicates");
            return Ok(Default::default());
        }

        // Create bulk write operations
        let namespace = db.namespace(crate::database::collections::CollectionName::MemberTransaction);
        let db_namespace = Namespace::from_str(&namespace).unwrap();
        let models: Vec<WriteModel> = new_transactions
            .iter()
            .map(|tx| {
                let document = bson::to_document(tx).unwrap();
                WriteModel::InsertOne(
                    InsertOneModel::builder()
                        .namespace(db_namespace.clone())
                        .document(document)
                        .build(),
                )
            })
            .collect();

        let result = db.client().bulk_write(models).await?;
        Ok(result)
    }

    /// Update member balances using HashMap lookup for efficiency
    pub async fn update_member_balances_bulk(
        balances: &[MemberBalance],
    ) -> Result<SummaryBulkWriteResult> {
        if balances.is_empty() {
            return Ok(Default::default());
        }

        let db = get_db_client().ok_or_else(|| anyhow::anyhow!("Global state not initialized"))?;

        // Check existing balances
        let db_ids: Vec<String> = balances
            .iter()
            .filter_map(|b| b.unique_id.clone())
            .collect();

        let existing_ids: HashSet<String> = db
            .member_balances()
            .find(doc! { "uniqueId": {"$in": &db_ids} })
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|doc| doc.unique_id)
            .collect();

        // Partition into updates and inserts
        let (updates, inserts): (Vec<_>, Vec<_>) = balances
            .iter()
            .filter_map(|b| {
                b.unique_id.clone().map(|id| {
                    let mut doc = bson::to_document(b).unwrap();
                    doc.insert("uniqueId", id.clone());
                    (id, doc)
                })
            })
            .partition(|(id, _)| existing_ids.contains(id));

        // Create bulk write operations
        let mut models = Vec::with_capacity(balances.len());
        let ns_str = db.namespace(crate::database::collections::CollectionName::MemberBalance);
        let ns = Namespace::from_str(&ns_str).unwrap();

        // Add update operations
        for (id, doc) in updates {
            models.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(ns.clone())
                    .filter(doc! {"uniqueId": id})
                    .update(doc! {"$set": doc})
                    .build(),
            ));
        }

        // Add insert operations
        for (_id, doc) in inserts {
            models.push(WriteModel::InsertOne(
                InsertOneModel::builder()
                    .namespace(ns.clone())
                    .document(doc)
                    .build(),
            ));
        }

        let result = db.client().bulk_write(models).await?;
        Ok(result)
    }

    /// Fetch ONLY balances from chain client for transfer events (SIMPLIFIED)
    pub async fn fetch_member_balances(
        network: &str,
        token_address: &Address,
        member_addresses: &[String],
        block_number: Option<u64>,
    ) -> HashMap<Address, String> {
        // Convert string addresses to Address type
        let addresses: Vec<Address> = member_addresses
            .iter()
            .filter_map(|addr| Address::from_str(addr).ok())
            .collect();

        if addresses.is_empty() {
            return HashMap::new();
        }

        match get_chain_client(network) {
            Some(chain_client) => {
                let client = chain_client.lock().await;
                match client.get_balances_batch(*token_address, &addresses, block_number).await {
                    Ok(batch_results) => {
                        info!("✅ Got on-chain balances for {} members", batch_results.len());
                        batch_results
                            .into_iter()
                            .map(|(addr, balance)| (addr, balance.to_string()))
                            .collect()
                    }
                    Err(e) => {
                        warn!("Batch balance request failed for {}: {}, falling back to DB", network, e);
                        HashMap::new()
                    }
                }
            }
            None => {
                warn!("No chain client for network {}, using DB balances", network);
                HashMap::new()
            }
        }
    }

    /// Create member balance records from fetched balances (for Transfer events)
    pub fn create_member_balance_records(
        network: &str,
        token_address: &str,
        member_addresses: &[String],
        balance_map: &HashMap<Address, String>,
        current_time: BsonDateTime,
    ) -> Vec<MemberBalance> {
        member_addresses
            .iter()
            .filter_map(|member_addr| {
                let address_parsed = Address::from_str(member_addr).ok()?;
                
                let balance = balance_map.get(&address_parsed)
                    .map(|bal| bal.clone())
                    .unwrap_or_else(|| "0".to_string());

                Some(MemberBalance {
                    id: None,
                    unique_id: Some(format!("{}-{}-{}", network, token_address, member_addr)),
                    network: network.to_string(),
                    token: token_address.to_string(),
                    member_address: member_addr.clone(),
                    balance,
                    voting_power: None, // Transfer events don't update voting power
                    updated_at: current_time,
                })
            })
            .collect()
    }

    /// Create member balance records with voting power from events (for Delegate events)
    /// NOTE: Delegate events don't need chain calls - voting power comes from event data
    pub fn create_delegate_balance_records(
        network: &str,
        token_address: &str,
        event_voting_powers: &HashMap<String, String>,
        current_time: BsonDateTime,
    ) -> Vec<MemberBalance> {
        event_voting_powers
            .iter()
            .map(|(member_addr, voting_power)| {
                MemberBalance {
                    id: None,
                    unique_id: Some(format!("{}-{}-{}", network, token_address, member_addr)),
                    network: network.to_string(),
                    token: token_address.to_string(),
                    member_address: member_addr.clone(),
                    balance: "0".to_string(), // Let DB compute balance later if needed
                    voting_power: Some(voting_power.clone()),
                    updated_at: current_time,
                }
            })
            .collect()
    }

    /// Process transactions and balances (shared save logic)
    pub async fn save_transactions_and_balances(
        member_txs: &[MemberTx],
        balance_updates: &[MemberBalance],
        event_type: &str,
        token_address: &str,
    ) -> Result<()> {
        // Save member transactions
        if !member_txs.is_empty() {
            let txs_len = member_txs.len();
            match Self::save_member_transactions_bulk(member_txs).await {
                Ok(result) => info!(
                    "✅ Saved {} {} transactions for token {} (inserted: {})",
                    txs_len, event_type, token_address, result.inserted_count
                ),
                Err(e) => {
                    warn!("Failed to save {} transactions for token {}: {}", event_type, token_address, e);
                    return Err(e);
                }
            }
        }

        // Update member balances
        if !balance_updates.is_empty() {
            let balance_len = balance_updates.len();
            match Self::update_member_balances_bulk(balance_updates).await {
                Ok(result) => info!(
                    "✅ Updated {} {} balances for token {} (modified: {}, upserted: {})",
                    balance_len, event_type, token_address, result.modified_count, result.upserted_count
                ),
                Err(e) => {
                    warn!("Failed to update {} balances for token {}: {}", event_type, token_address, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

