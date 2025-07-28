use bson::{doc, oid::ObjectId, DateTime as BsonDateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    #[serde(rename = "uniqueId")]
    pub unique_id: String, // Format: transactionHash-network-txIndex-logIndex-address
    pub network: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "txHash")]
    pub tx_hash: String,
    #[serde(rename = "txIndex")]
    pub tx_index: u32,
    #[serde(rename = "logIndex")]
    pub log_index: u32,
    #[serde(rename = "topicHash")]
    pub topic_hash: String,
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_signature: Option<String>, // Human-readable event signature
    #[serde(rename = "createdAt")]
    pub created_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberTx {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    #[serde(rename = "uniqueId", skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    pub network: String,
    pub token: String,
    #[serde(rename = "memberAddress")]
    pub member_address: String,
    pub direction: TxDirection,
    pub block_number: u64,
    pub tx_hash: String,
    pub value: Option<String>,            // U256 as string
    pub previous_balance: Option<String>, // For delegate events
    pub new_balance: Option<String>,      // For delegate events
    pub created_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxDirection {
    Incoming,
    Outgoing,
    Delegate,
    NativeTransfer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberBalance {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    #[serde(rename = "uniqueId", skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    pub network: String,
    pub token: String,
    #[serde(rename = "memberAddress")]
    pub member_address: String,
    pub balance: String,              // U256 as string
    pub voting_power: Option<String>, // U256 as string
    pub updated_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigIndexer {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub network: String,
    pub token: String,
    pub last_synced_block: u64,
    pub last_synced_tx_index: u32,
    pub last_synced_log_index: u32,
    pub updated_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncJob {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub job_id: String, // UUID as string
    pub network: String,
    pub token_address: String,
    pub from_block: u64,
    pub to_block: u64,
    pub status: JobStatus,
    pub created_at: BsonDateTime,
    pub updated_at: BsonDateTime,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalSyncRequest {
    pub network: String,
    #[serde(rename = "tokenAddress")]
    pub token_address: String,
    #[serde(rename = "fromBlock")]
    pub from_block: u64,
    #[serde(rename = "toBlock")]
    pub to_block: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub id: String, // UUID as string
    pub status: JobStatus,
    pub progress: Option<f64>,
    pub error: Option<String>,
}

// DAO model for tracking DAO addresses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dao {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub address: String,
    pub network: String,
    pub name: Option<String>,
    pub creator: Option<String>,
    pub subdomain: Option<String>,
    pub created_at: BsonDateTime,
    pub updated_at: BsonDateTime,
}

// Plugin model for tracking plugin token addresses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plugin {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub address: String,
    pub token_address: String,
    pub network: String,
    pub status: PluginStatus,
    pub is_supported: bool,
    pub interface_type: PluginInterfaceType,
    pub created_at: BsonDateTime,
    pub updated_at: BsonDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginStatus {
    Installed,
    Uninstalled,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PluginInterfaceType {
    TokenVoting,
    Multisig,
    AddresslistVoting,
    Admin,
}

// Token model for tracking valid token addresses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub address: String,
    pub network: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub ignore_transfer: bool,
    pub created_at: BsonDateTime,
    pub updated_at: BsonDateTime,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let event = Event {
            id: None,
            unique_id: "0xabcdef-test-network-1-2-0x789abc".to_string(),
            network: "test-network".to_string(),
            block_number: 12345,
            tx_hash: "0xabcdef".to_string(),
            tx_index: 1,
            log_index: 2,
            topic_hash: "0x123456".to_string(),
            address: "0x789abc".to_string(),
            data: "0xdata".to_string(),
            topics: vec!["0xtopic1".to_string()],
            event_signature: Some("Transfer(address,address,uint256)".to_string()),
            created_at: BsonDateTime::now(),
        };

        // Test BSON serialization (what MongoDB expects)
        let bson_doc = bson::to_document(&event).expect("Failed to serialize to BSON");
        
        // Verify the field names match the database index
        assert!(bson_doc.contains_key("uniqueId"));
        assert!(bson_doc.contains_key("network"));
        assert!(bson_doc.contains_key("address"));
        assert!(bson_doc.contains_key("txHash"));
        assert!(bson_doc.contains_key("blockNumber"));
        assert!(bson_doc.contains_key("txIndex"));
        assert!(bson_doc.contains_key("logIndex"));
        assert!(bson_doc.contains_key("topicHash"));
        
        // Verify values are not null
        assert!(bson_doc.get("uniqueId").unwrap().as_null().is_none());
        assert!(bson_doc.get("network").unwrap().as_null().is_none());
        assert!(bson_doc.get("address").unwrap().as_null().is_none());
        assert!(bson_doc.get("txHash").unwrap().as_null().is_none());
        assert!(bson_doc.get("blockNumber").unwrap().as_null().is_none());
        assert!(bson_doc.get("txIndex").unwrap().as_null().is_none());
        assert!(bson_doc.get("logIndex").unwrap().as_null().is_none());
        assert!(bson_doc.get("topicHash").unwrap().as_null().is_none());

        println!("Event serialized correctly: {:?}", bson_doc);
    }
}
