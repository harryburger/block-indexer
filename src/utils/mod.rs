use anyhow::Result;
use tracing::info;
use ethers::types::Address;
use ethers::utils::to_checksum;
use std::str::FromStr;

pub fn setup_logging() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("block_injector=info".parse()?)
                .add_directive("info".parse()?)
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    info!("Logging initialized");
    Ok(())
}

pub fn validate_ethereum_address(address: &str) -> bool {
    if !address.starts_with("0x") {
        return false;
    }
    
    if address.len() != 42 {
        return false;
    }
    
    address[2..].chars().all(|c| c.is_ascii_hexdigit())
}

pub fn validate_topic_hash(topic: &str) -> bool {
    if !topic.starts_with("0x") {
        return false;
    }
    
    if topic.len() != 66 {
        return false;
    }
    
    topic[2..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Extract and convert address from 32-byte event topic to checksummed format
/// Used for governance events where addresses are stored in topics
pub fn extract_checksummed_address_from_topic(topic: &str) -> Option<String> {
    if topic.len() >= 66 {
        let addr_hex = &topic[26..66]; // Extract 20-byte address from 32-byte topic
        if let Ok(address) = Address::from_str(&format!("0x{}", addr_hex)) {
            return Some(to_checksum(&address, None));
        }
    }
    None
}

/// Normalize event signature by removing parameter names and extra spaces
/// Useful for handling both ABI formats: with and without parameter names
pub fn normalize_event_signature(signature: &str) -> String {
    // Remove common parameter names and normalize spaces
    signature
        .replace(" from", "")
        .replace(" to", "")
        .replace(" value", "")
        .replace(" delegate", "")
        .replace(" previousBalance", "")
        .replace(" newBalance", "")
        .replace(" ", "")
}
