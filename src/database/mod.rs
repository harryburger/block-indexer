pub mod collections;

use crate::chain_client::ChainClient;
use crate::config::AppSettings;
use crate::models::*;
use anyhow::Result;
use bson::doc;
use collections::CollectionName;
use futures::stream::TryStreamExt;
use mongodb::options::{ClientOptions, IndexOptions, UpdateOptions};
use mongodb::{Client, Collection, Database, IndexModel};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Registry for chain clients accessible by network name
pub type ChainClientRegistry = Arc<HashMap<String, Arc<Mutex<ChainClient>>>>;

#[derive(Clone)]
pub struct DatabaseClient {
    pub client: Client,
    pub database: Database,
    pub database_name: String,
    /// Optional reference to chain clients for on-chain balance fetching
    pub chain_clients: Option<ChainClientRegistry>,
}

impl DatabaseClient {
    pub async fn new(connection_string: &str, database_name: &str) -> Result<Self> {
        Self::new_with_config(connection_string, database_name, &AppSettings {
            name: "default".to_string(),
            version: "1.0.0".to_string(),
            health_check_interval_ms: 30000,
            summary_interval_seconds: 60,
            mongodb_url: None,
            mongodb_database: None,
            source_mongodb_url: None,
            source_mongodb_database: None,
            mongodb_max_pool_size: Some(20),
            mongodb_min_pool_size: Some(5),
            mongodb_max_idle_time_seconds: Some(300),
        }).await
    }

    pub async fn new_with_config(connection_string: &str, database_name: &str, config: &AppSettings) -> Result<Self> {
        // Parse connection string and configure connection pooling
        let mut client_options = ClientOptions::parse(connection_string).await?;
        
        // Configure connection pool settings from config or defaults
        client_options.max_pool_size = Some(config.mongodb_max_pool_size.unwrap_or(20));
        client_options.min_pool_size = Some(config.mongodb_min_pool_size.unwrap_or(5)); 
        client_options.max_idle_time = Some(Duration::from_secs(config.mongodb_max_idle_time_seconds.unwrap_or(300)));
        client_options.max_connecting = Some(5);          // Max concurrent connection attempts
        client_options.connect_timeout = Some(Duration::from_secs(10)); // Connection timeout
        client_options.server_selection_timeout = Some(Duration::from_secs(5)); // Server selection timeout
        
        // Enable connection monitoring for better diagnostics
        client_options.heartbeat_freq = Some(Duration::from_secs(10));
        
        info!(
            "🔗 Initializing MongoDB connection pool for database '{}' with {} max connections",
            database_name, client_options.max_pool_size.unwrap_or(100)
        );

        let client = Client::with_options(client_options)?;
        let database = client.database(database_name);

        // Test connection pool by performing a ping
        match client.database("admin").run_command(doc! { "ping": 1 }).await {
            Ok(_) => info!("✅ MongoDB connection pool established successfully"),
            Err(e) => {
                warn!("⚠️ MongoDB connection pool test failed: {}", e);
                return Err(anyhow::anyhow!("Failed to establish connection pool: {}", e));
            }
        }

        let db_client = Self {
            client,
            database,
            database_name: database_name.to_string(),
            chain_clients: None,
        };

        // Create indexes
        db_client.create_indexes().await?;

        Ok(db_client)
    }

    /// Set chain clients registry for on-chain operations
    pub fn with_chain_clients(mut self, chain_clients: ChainClientRegistry) -> Self {
        self.chain_clients = Some(chain_clients);
        self
    }

    /// Get chain client for a specific network
    pub fn get_chain_client(&self, network: &str) -> Option<Arc<Mutex<ChainClient>>> {
        self.chain_clients.as_ref()?.get(network).cloned()
    }

    /// Get MongoDB client for bulk operations
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get collection namespace for bulk operations
    pub fn namespace(&self, collection: CollectionName) -> String {
        collection.namespace(&self.database_name)
    }

    /// Get connection pool statistics for monitoring
    pub async fn get_pool_stats(&self) -> Result<String> {
        // Try to get server status for connection pool information
        match self.client.database("admin").run_command(doc! { "serverStatus": 1 }).await {
            Ok(result) => {
                if let Some(connections) = result.get("connections") {
                    Ok(format!("MongoDB Pool Stats: {}", connections))
                } else {
                    Ok("Connection pool information not available".to_string())
                }
            }
            Err(e) => {
                warn!("Failed to get connection pool stats: {}", e);
                Ok("Failed to retrieve pool stats".to_string())
            }
        }
    }

    /// Health check method that uses connection pool efficiently
    pub async fn health_check(&self) -> Result<bool> {
        match self.client.database("admin").run_command(doc! { "ping": 1 }).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn create_indexes(&self) -> Result<()> {
        // Create unique index for events to prevent duplicates
        let events: Collection<Event> = self.database.collection(CollectionName::Event.as_str());
        let index_model = IndexModel::builder()
            .keys(doc! {
                "network": 1,
                "address": 1,
                "txHash": 1,
                "blockNumber": 1,
                "txIndex": 1,
                "logIndex": 1,
                "topicHash": 1
            })
            .options(Some(IndexOptions::builder().unique(true).build()))
            .build();
        events.create_index(index_model).await?;

        // Create index for member transactions
        let member_tx: Collection<MemberTx> = self
            .database
            .collection(CollectionName::MemberTransaction.as_str());
        let index_model = IndexModel::builder()
            .keys(doc! {
                "network": 1,
                "token": 1,
                "memberAddress": 1,
                "direction": 1,
                "blockNumber": 1
            })
            .options(Some(IndexOptions::builder().build()))
            .build();
        member_tx.create_index(index_model).await?;

        // Create unique index for member balances
        let member_balances: Collection<MemberBalance> = self
            .database
            .collection(CollectionName::MemberBalance.as_str());
        let index_model = IndexModel::builder()
            .keys(doc! {
                "network": 1,
                "token": 1,
                "memberAddress": 1
            })
            .options(Some(IndexOptions::builder().unique(true).build()))
            .build();
        member_balances.create_index(index_model).await?;

        // Create unique index for config indexers
        let config_indexers: Collection<ConfigIndexer> = self
            .database
            .collection(CollectionName::ConfigIndexer.as_str());
        let index_model = IndexModel::builder()
            .keys(doc! {
                "network": 1,
                "token": 1
            })
            .options(Some(IndexOptions::builder().unique(true).build()))
            .build();
        config_indexers.create_index(index_model).await?;

        Ok(())
    }

    pub fn events(&self) -> Collection<Event> {
        self.database.collection(CollectionName::Event.as_str())
    }

    pub fn member_tx(&self) -> Collection<MemberTx> {
        self.database
            .collection(CollectionName::MemberTransaction.as_str())
    }

    pub fn member_balances(&self) -> Collection<MemberBalance> {
        self.database
            .collection(CollectionName::MemberBalance.as_str())
    }

    pub fn config_indexers(&self) -> Collection<ConfigIndexer> {
        self.database
            .collection(CollectionName::ConfigIndexer.as_str())
    }

    pub fn sync_jobs(&self) -> Collection<SyncJob> {
        self.database.collection(CollectionName::SyncJob.as_str())
    }

    pub fn daos(&self) -> Collection<Dao> {
        self.database.collection(CollectionName::Dao.as_str())
    }

    pub fn plugins(&self) -> Collection<Plugin> {
        self.database.collection(CollectionName::Plugin.as_str())
    }

    pub fn tokens(&self) -> Collection<Token> {
        self.database.collection(CollectionName::Token.as_str())
    }

    pub async fn get_last_synced_block(&self, network: &str, token: &str) -> Result<u64> {
        let collection = self.config_indexers();

        if let Some(config) = collection
            .find_one(doc! { "network": network, "token": token })
            .await?
        {
            Ok(config.last_synced_block)
        } else {
            Ok(0) // Start from block 0 if no previous sync
        }
    }

    pub async fn update_last_synced_block(
        &self,
        network: &str,
        token: &str,
        block_number: u64,
        _tx_index: u32,
        _log_index: u32,
    ) -> Result<()> {
        let collection = self.config_indexers();

        let _options = UpdateOptions::builder().upsert(true).build();
        collection
            .update_one(
                doc! { "network": network, "token": token },
                doc! {
                    "$set": {
                        "last_processed_block": block_number as i64,
                        "updated_at": chrono::Utc::now()
                    }
                },
            )
            .await?;

        Ok(())
    }

    pub async fn compute_balance(
        &self,
        network: &str,
        token: &str,
        address: &str,
    ) -> Result<String> {
        let collection = self.member_tx();

        // Aggregate incoming transfers
        let incoming_pipeline = vec![
            doc! {
                "$match": {
                    "network": network,
                    "token": token,
                    "memberAddress": address,
                    "direction": "incoming"
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "total": { "$sum": { "$toDecimal": "$value" } }
                }
            },
        ];

        let mut incoming_cursor = collection.aggregate(incoming_pipeline).await?;
        let mut incoming_total = 0u128;

        if let Some(result) = incoming_cursor.try_next().await? {
            if let Some(total) = result.get("total") {
                if let Some(decimal_str) = total.as_str() {
                    incoming_total = decimal_str.parse().unwrap_or(0);
                }
            }
        }

        // Aggregate outgoing transfers
        let outgoing_pipeline = vec![
            doc! {
                "$match": {
                    "network": network,
                    "token": token,
                    "memberAddress": address,
                    "direction": "outgoing"
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "total": { "$sum": { "$toDecimal": "$value" } }
                }
            },
        ];

        let mut outgoing_cursor = collection.aggregate(outgoing_pipeline).await?;
        let mut outgoing_total = 0u128;

        if let Some(result) = outgoing_cursor.try_next().await? {
            if let Some(total) = result.get("total") {
                if let Some(decimal_str) = total.as_str() {
                    outgoing_total = decimal_str.parse().unwrap_or(0);
                }
            }
        }

        let balance = incoming_total.saturating_sub(outgoing_total);
        Ok(balance.to_string())
    }

    // Query methods for address filtering like the JavaScript code
    pub async fn get_dao_addresses(
        &self,
        network: &str,
        addresses: &[String],
    ) -> Result<Vec<String>> {
        let collection = self.daos();
        let filter = doc! {
            "address": { "$in": addresses },
            "network": network
        };

        let mut cursor = collection.find(filter).await?;
        let mut dao_addresses = Vec::new();

        while let Some(dao) = cursor.try_next().await? {
            dao_addresses.push(dao.address);
        }

        Ok(dao_addresses)
    }

    pub async fn get_plugin_token_addresses(
        &self,
        network: &str,
        token_addresses: &[String],
    ) -> Result<Vec<String>> {
        let collection = self.plugins();
        let filter = doc! {
            "tokenAddress": { "$in": token_addresses },
            "status": "installed",
            "isSupported": true,
            "interfaceType": "tokenVoting",
            "network": network
        };

        let mut cursor = collection.find(filter).await?;
        let mut plugin_token_addresses = Vec::new();

        while let Some(plugin) = cursor.try_next().await? {
            plugin_token_addresses.push(plugin.token_address);
        }

        Ok(plugin_token_addresses)
    }

    pub async fn get_valid_token_addresses(
        &self,
        network: &str,
        addresses: &[String],
    ) -> Result<Vec<String>> {
        let collection = self.tokens();
        let filter = doc! {
            "address": { "$in": addresses },
            "ignoreTransfer": { "$ne": true },
            "network": network
        };

        let mut cursor = collection.find(filter).await?;
        let mut valid_token_addresses = Vec::new();

        while let Some(token) = cursor.try_next().await? {
            valid_token_addresses.push(token.address);
        }

        Ok(valid_token_addresses)
    }
}
