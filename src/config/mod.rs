use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub app: AppSettings,
    pub chains: HashMap<String, ChainConfig>,
    pub topics: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppSettings {
    pub name: String,
    pub version: String,
    pub health_check_interval_ms: u64,
    pub summary_interval_seconds: u64,
    pub mongodb_url: Option<String>,
    pub mongodb_database: Option<String>,
    pub source_mongodb_url: Option<String>,
    pub source_mongodb_database: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChainConfig {
    pub enabled: bool,
    pub rpc_url: String,
    pub poll_interval_seconds: u64,
    pub finality_wait_seconds: u64,
    pub batch_size: Option<u64>,
}

impl AppConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;

        Ok(settings.try_deserialize()?)
    }

    pub fn get_enabled_chains(&self) -> Vec<(String, ChainConfig)> {
        self.chains
            .iter()
            .filter(|(_, config)| config.enabled)
            .map(|(name, config)| (name.clone(), config.clone()))
            .collect()
    }

    pub fn get_topic_keys(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }
}
