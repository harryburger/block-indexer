use std::collections::HashMap;
use std::sync::Arc;

use ethers::types::{Block, Log, Transaction, H256};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::chain_client::{ChainClient, ChainClientTrait};
use crate::config::AppConfig;
use crate::error::InjectorError;
use crate::processor::LogProcessor;

pub struct EventListener {
    config: AppConfig,
    clients: HashMap<String, ChainClient>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl EventListener {
    pub fn new(
        config: AppConfig,
        _db: std::sync::Arc<crate::database::DatabaseClient>,
        _source_db: Option<crate::source::MongoClient>,
    ) -> Self {
        Self {
            config,
            clients: HashMap::new(),
            shutdown_tx: None,
        }
    }

    /// Initialize chain clients
    pub async fn initialize(&mut self) -> Result<(), InjectorError> {
        info!("Initializing event listener for {} chains", self.config.chains.len());

        for (name, chain_config) in &self.config.chains {
            if !chain_config.enabled {
                info!("Skipping disabled chain: {}", name);
                continue;
            }

            info!("Initializing chain client: {}", name);
            let mut client = ChainClient::new(name.clone(), chain_config.clone());

            // Set topic filters
            let topic_keys = self.config.get_topic_keys();
            if !topic_keys.is_empty() {
                let count = topic_keys.len();
                client.set_topic_filters(topic_keys);
                info!("Applied {} topic filters for chain {}", count, name);
            }

            // Connect
            client.connect().await?;
            self.clients.insert(name.clone(), client);
            info!("✅ Successfully initialized client for chain: {}", name);
        }

        Ok(())
    }

    /// Start listening to all enabled chains
    pub async fn start(&mut self) -> Result<(), InjectorError> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        info!("🚀 Starting event listener for {} chains", self.clients.len());

        // Create processor
        let chain_clients = self.get_chain_clients_for_processor();
        let processor = Arc::new(
            LogProcessor::new(
                self.config.clone(),
                chain_clients,
            )
            .await,
        );

        // Start polling for each chain in separate tokio tasks
        let mut handles = Vec::new();
        let clients = std::mem::take(&mut self.clients);
        
        for (chain_name, mut client) in clients {
            let processor_clone = processor.clone();
            let chain_name_clone = chain_name.clone();

            let handle = tokio::spawn(async move {
                let callback = move |chain: &str, block: &Block<H256>, logs: &[Log], _: &[Transaction]| {
                    let block_number = block.number.unwrap_or_default().as_u64();
                    info!("🔍 Chain {}: New block #{} with {} logs", chain, block_number, logs.len());

                    // Process asynchronously
                    let processor = processor_clone.clone();
                    let chain_name = chain.to_string();
                    let logs = logs.to_vec();

                    tokio::spawn(async move {
                        if let Err(e) = processor
                            .process_logs(&chain_name, block_number, &logs)
                            .await
                        {
                            error!("❌ Failed to process logs for block #{}: {}", block_number, e);
                        }
                    });
                };

                if let Err(e) = client.start_polling(callback).await {
                    error!("❌ Polling failed for chain {}: {}", chain_name_clone, e);
                } else {
                    info!("✅ Started polling for chain: {}", chain_name_clone);
                }
            });

            handles.push(handle);
        }

        info!("🎯 All {} chain tasks started, waiting for shutdown signal...", handles.len());

        // Wait for shutdown signal
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal");
                // Abort all chain polling tasks
                for handle in handles {
                    handle.abort();
                }
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down gracefully");
                // Abort all chain polling tasks
                for handle in handles {
                    handle.abort();
                }
            },
        }

        Ok(())
    }

    /// Get access to the chain clients registry (for app state)
    pub fn get_chain_clients(&self) -> Arc<HashMap<String, Arc<tokio::sync::Mutex<ChainClient>>>> {
        let mut clients = HashMap::new();
        for (name, client) in &self.clients {
            clients.insert(name.clone(), Arc::new(tokio::sync::Mutex::new(client.clone())));
        }
        Arc::new(clients)
    }

    /// Get chain clients for processor
    fn get_chain_clients_for_processor(&self) -> Arc<HashMap<String, Arc<tokio::sync::Mutex<ChainClient>>>> {
        self.get_chain_clients()
    }

    /// Shutdown the event listener
    pub async fn shutdown(&mut self) {
        info!("🛑 Shutting down event listener...");
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        info!("✅ Event listener shutdown completed");
    }
}