use anyhow::Result;
use tracing::info;

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
