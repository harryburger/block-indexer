use anyhow::Result;
use bson::Document;
use futures::stream::TryStreamExt;
use mongodb::{Client, Collection, Database};
use serde_json::Value;
use tracing::info;

#[derive(Clone)]
pub struct MongoClient {
    pub database: Database,
}

impl MongoClient {
    /// Create a new MongoDB connection
    pub async fn new(connection_string: &str, database_name: &str) -> Result<Self> {
        let client = Client::with_uri_str(connection_string).await?;
        let database = client.database(database_name);

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
