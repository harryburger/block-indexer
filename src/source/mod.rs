use anyhow::Result;
use bson::Document;
use futures::stream::TryStreamExt;
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection, Database};
use serde_json::Value;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Clone)]
pub struct MongoClient {
    pub database: Database,
}

impl MongoClient {
    /// Create a new MongoDB connection with connection pooling
    pub async fn new(connection_string: &str, database_name: &str) -> Result<Self> {
        // Parse connection string and configure connection pooling for source DB
        let mut client_options = ClientOptions::parse(connection_string).await?;
        
        // Configure smaller pool for source database (read-only operations)
        client_options.max_pool_size = Some(10);          // Smaller pool for source DB
        client_options.min_pool_size = Some(2);           // Minimum connections
        client_options.max_idle_time = Some(Duration::from_secs(600)); // 10 minutes idle timeout
        client_options.max_connecting = Some(3);          // Max concurrent connection attempts
        client_options.connect_timeout = Some(Duration::from_secs(10)); // Connection timeout
        client_options.server_selection_timeout = Some(Duration::from_secs(5)); // Server selection timeout
        
        info!(
            "🔗 Initializing source MongoDB connection pool for database '{}' with {} max connections",
            database_name, client_options.max_pool_size.unwrap_or(10)
        );

        let client = Client::with_options(client_options)?;
        let database = client.database(database_name);

        // Test connection pool
        match client.database("admin").run_command(bson::doc! { "ping": 1 }).await {
            Ok(_) => info!("✅ Source MongoDB connection pool established successfully"),
            Err(e) => {
                warn!("⚠️ Source MongoDB connection pool test failed: {}", e);
                return Err(anyhow::anyhow!("Failed to establish source connection pool: {}", e));
            }
        }

        Ok(Self { database })
    }

    /// Execute a find query on any model/collection with JSON query
    pub async fn query(&self, model: &str, query: Value) -> Result<Vec<Document>> {
        let collection: Collection<Document> = self.database.collection(model);

        // Convert JSON query to BSON document using bson::to_document
        let filter: Document = match bson::to_document(&query) {
            Ok(doc) => doc,
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to convert query to BSON: {}", e));
            }
        };

        // Execute the query and get all documents at once
        let results = collection
            .find(filter)
            .await?
            .try_collect::<Vec<Document>>()
            .await?;

        Ok(results)
    }

    /// Execute an aggregation pipeline on any model/collection with JSON pipeline
    pub async fn aggregate(&self, model: &str, pipeline: Vec<Value>) -> Result<Vec<Document>> {
        let collection: Collection<Document> = self.database.collection(model);

        // Convert JSON pipeline to BSON documents
        let mut bson_pipeline = Vec::new();
        for stage in pipeline {
            let bson_stage: Document = bson::to_document(&stage)?;
            bson_pipeline.push(bson_stage);
        }

        // Execute aggregation and collect all results at once
        let results = collection
            .aggregate(bson_pipeline)
            .await?
            .try_collect::<Vec<Document>>()
            .await?;

        Ok(results)
    }

    /// List all available collections in the database
    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let collection_names = self.database.list_collection_names().await?;
        Ok(collection_names)
    }

    /// Check if a collection exists and has documents
    pub async fn collection_info(&self, collection_name: &str) -> Result<(bool, u64)> {
        let collection: Collection<Document> = self.database.collection(collection_name);

        match collection.estimated_document_count().await {
            Ok(count) => {
                info!(
                    "Collection '{}' exists with ~{} documents",
                    collection_name, count
                );
                Ok((true, count))
            }
            Err(e) => {
                tracing::debug!("Collection '{}' check failed: {}", collection_name, e);
                Ok((false, 0))
            }
        }
    }

    /// Query distinct token addresses from plugins collection for a specific network
    pub async fn query_plugin_tokens(&self, network: &str) -> Result<Vec<String>> {
        info!(
            "🔍 Querying distinct token addresses from plugins for network: {}",
            network
        );

        let collection = self.database.collection::<Document>("Plugin");

        let pipeline = vec![
            bson::doc! {
                "$match": {
                    "status": "installed",
                    "isSupported": true,
                    "interfaceType": "tokenVoting",
                    "network": network
                }
            },
            bson::doc! {
                "$group": {
                    "_id": "$tokenAddress"
                }
            },
            bson::doc! {
                "$project": {
                    "_id": 0,
                    "tokenAddress": "$_id"
                }
            },
        ];

        let mut cursor = collection.aggregate(pipeline).await?;
        let mut token_addresses = Vec::new();

        use futures::stream::TryStreamExt;
        while let Some(doc) = cursor.try_next().await? {
            if let Ok(address) = doc.get_str("tokenAddress") {
                token_addresses.push(address.to_string());
            }
        }

        info!(
            "✅ Found {} distinct token addresses for network {} from plugins",
            token_addresses.len(),
            network
        );
        Ok(token_addresses)
    }
}
