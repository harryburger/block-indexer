use crate::database::DatabaseClient;
use crate::handlers::{EventHandler, HandlerRegistry};
use crate::models::*;
use anyhow::Result;
use bson::{doc, DateTime as BsonDateTime};
use ethers::types::Address;
use ethers::utils::to_checksum;
use futures::stream::TryStreamExt;
use mongodb::{
    options::{InsertOneModel, UpdateOneModel, WriteModel},
    results::SummaryBulkWriteResult,
    Namespace,
};
use std::collections::HashMap;
use std::str::FromStr;
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

    /// Extract and convert address from topic to checksummed format
    fn extract_checksummed_address(topic: &str) -> Option<String> {
        if topic.len() >= 66 {
            let addr_hex = &topic[26..66]; // Extract 20-byte address from 32-byte topic
            if let Ok(address) = Address::from_str(&format!("0x{}", addr_hex)) {
                return Some(to_checksum(&address, None));
            }
        }
        None
    }

    async fn save_member_transactions_bulk(
        &self,
        db: &DatabaseClient,
        member_txs: &[MemberTx],
    ) -> Result<SummaryBulkWriteResult> {
        let namespace =
            db.namespace(crate::database::collections::CollectionName::MemberTransaction);
        let db_namespace = Namespace::from_str(&namespace).unwrap();
        let models: Vec<WriteModel> = member_txs
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

    async fn update_member_balances_bulk(
        &self,
        db: &DatabaseClient,
        balance_updates: &[MemberBalance],
    ) -> Result<SummaryBulkWriteResult> {
        if balance_updates.is_empty() {
            return Ok(Default::default());
        }

        // Create filters to check which balances already exist
        let filters: Vec<_> = balance_updates
            .iter()
            .map(|balance| {
                doc! {
                    "network": &balance.network,
                    "token": &balance.token,
                    "memberAddress": &balance.member_address,
                }
            })
            .collect();

        // Find existing balances
        let existing_filter = doc! {
            "$or": filters
        };

        let mut cursor = db.member_balances().find(existing_filter).await?;
        let mut existing_keys = std::collections::HashSet::new();

        while let Some(existing_balance) = cursor.try_next().await? {
            let key = format!(
                "{}-{}-{}",
                existing_balance.network, existing_balance.token, existing_balance.member_address
            );
            existing_keys.insert(key);
        }

        // Separate into insert and update operations
        let mut models = Vec::new();
        let balance_namespace_str =
            db.namespace(crate::database::collections::CollectionName::MemberBalance);
        let balance_namespace = Namespace::from_str(&balance_namespace_str).unwrap();

        for balance in balance_updates {
            let key = format!(
                "{}-{}-{}",
                balance.network, balance.token, balance.member_address
            );

            if existing_keys.contains(&key) {
                // Update existing balance
                let filter = doc! {
                    "network": &balance.network,
                    "token": &balance.token,
                    "memberAddress": &balance.member_address,
                };
                let update = doc! { "$set": bson::to_document(balance).unwrap() };
                models.push(WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(balance_namespace.clone())
                        .filter(filter)
                        .update(update)
                        .build(),
                ));
            } else {
                // Insert new balance
                let document = bson::to_document(balance).unwrap();
                models.push(WriteModel::InsertOne(
                    InsertOneModel::builder()
                        .namespace(balance_namespace.clone())
                        .document(document)
                        .build(),
                ));
            }
        }

        let result = db.client().bulk_write(models).await?;
        Ok(result)
    }
}

#[async_trait::async_trait]
impl EventHandler for GovTokenDelegateHandler {
    async fn handle_batch(
        &self,
        events: Vec<Event>,
        db: &DatabaseClient,
        registry: &HandlerRegistry,
    ) -> Result<()> {
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

            let Some(delegate_addr) = Self::extract_checksummed_address(&delegate) else {
                continue;
            };

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

        // Save member transactions using bulk_write
        if !member_txs.is_empty() {
            let txs_len = member_txs.len();
            match self.save_member_transactions_bulk(db, &member_txs).await {
                Ok(result) => info!(
                    "✅ Saved {} delegate transactions (inserted: {})",
                    txs_len, result.inserted_count
                ),
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
                    debug!(
                        "🔗 Fetching on-chain data for {} on {} token {}",
                        address, network, token
                    );

                    // Fetch both balance and voting power from chain
                    let balance_result = chain_client.get_token_balance(&token, &address).await;
                    let votes_result = chain_client.get_voting_power(&token, &address).await;

                    let balance = match balance_result {
                        Ok(bal) => {
                            debug!("✅ Got balance {} for {} on {}", bal, address, token);
                            bal
                        }
                        Err(e) => {
                            warn!(
                                "Failed to fetch on-chain balance for {}: {}, using event data",
                                address, e
                            );
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
                    warn!(
                        "No chain client for network {}, using event data and DB fallback",
                        network
                    );

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
            match self.update_member_balances_bulk(db, &balance_updates).await {
                Ok(result) => info!(
                    "✅ Updated {} delegate balances (modified: {}, upserted: {})",
                    balance_len, result.modified_count, result.upserted_count
                ),
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
