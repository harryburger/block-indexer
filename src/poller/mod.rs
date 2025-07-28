use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ethers::types::{Block, Log, Transaction, H256};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::chain_client::{ChainClient, ChainClientTrait, TopicStats};
use crate::config::AppConfig;
use crate::error::InjectorError;
use crate::processor::LogProcessor; // Changed from TokenProcessor

/// Health status for a chain client
#[derive(Debug, Clone)]
pub struct ChainHealth {
    pub name: String,
    pub is_healthy: bool,
    pub last_successful_poll: Option<std::time::Instant>,
    pub consecutive_failures: u32,
    pub total_failures: u64,
    pub total_successes: u64,
}

impl ChainHealth {
    pub fn new(name: String) -> Self {
        Self {
            name,
            is_healthy: true,
            last_successful_poll: None,
            consecutive_failures: 0,
            total_failures: 0,
            total_successes: 0,
        }
    }

    pub fn record_success(&mut self) {
        self.is_healthy = true;
        self.last_successful_poll = Some(std::time::Instant::now());
        self.consecutive_failures = 0;
        self.total_successes += 1;
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.total_failures += 1;

        // Mark as unhealthy after 3 consecutive failures
        if self.consecutive_failures >= 3 {
            self.is_healthy = false;
        }
    }

    pub fn is_stale(&self, max_age: Duration) -> bool {
        match self.last_successful_poll {
            Some(last_poll) => last_poll.elapsed() > max_age,
            None => true, // Never polled successfully
        }
    }
}

pub struct EventListener {
    config: AppConfig,
    db: std::sync::Arc<crate::database::DatabaseClient>,
    source_db: Option<crate::source::MongoClient>, // Add this
    clients: HashMap<String, Arc<Mutex<ChainClient>>>,
    topic_stats: Arc<RwLock<HashMap<String, HashMap<String, usize>>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Health monitoring for each chain
    chain_health: Arc<RwLock<HashMap<String, ChainHealth>>>,
    /// Global health check task handle
    health_monitor_handle: Option<tokio::task::JoinHandle<()>>,
    /// Recovery manager for failed chains
    recovery_manager_handle: Option<tokio::task::JoinHandle<()>>,
}

impl EventListener {
    pub fn new(
        config: AppConfig,
        db: std::sync::Arc<crate::database::DatabaseClient>,
        source_db: Option<crate::source::MongoClient>, // Add this parameter
    ) -> Self {
        Self {
            config,
            db,
            source_db, // Add this
            clients: HashMap::new(),
            topic_stats: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
            chain_health: Arc::new(RwLock::new(HashMap::new())),
            health_monitor_handle: None,
            recovery_manager_handle: None,
        }
    }

    /// Get access to the chain clients registry (for app state)
    pub fn get_chain_clients(&self) -> Arc<HashMap<String, Arc<Mutex<ChainClient>>>> {
        Arc::new(self.clients.clone())
    }

    /// Initialize all chain clients with comprehensive error handling
    pub async fn initialize(&mut self) -> Result<(), InjectorError> {
        info!(
            "Initializing resilient event listener with {} chains",
            self.config.chains.len()
        );

        let mut initialization_errors = Vec::new();
        let mut successfully_initialized = 0;

        for (name, chain_config) in &self.config.chains {
            if !chain_config.enabled {
                info!("Skipping disabled chain: {}", name);
                continue;
            }

            info!("Initializing chain client: {}", name);

            // Create health tracker
            let mut health = self.chain_health.write().await;
            health.insert(name.clone(), ChainHealth::new(name.clone()));
            drop(health);

            // Create and configure client
            let mut client = ChainClient::new(name.clone(), chain_config.clone());

            // Use global topics list from config (extract keys from the HashMap)
            let topic_keys = self.config.get_topic_keys();
            if !topic_keys.is_empty() {
                client.set_topic_filters(topic_keys.clone());
                info!(
                    "Applied {} global topic filters for chain {} (with human-readable signatures)",
                    topic_keys.len(),
                    name
                );
            } else {
                warn!(
                    "No global topics configured, will listen to all events on chain {}",
                    name
                );
            }

            // Attempt connection with retry
            match self.connect_with_retry(&mut client, name, 3).await {
                Ok(_) => {
                    self.clients
                        .insert(name.clone(), Arc::new(Mutex::new(client)));
                    successfully_initialized += 1;
                    info!("✅ Successfully initialized client for chain: {}", name);

                    // Initialize stats tracker for this chain
                    let mut stats = self.topic_stats.write().await;
                    stats.insert(name.clone(), HashMap::new());
                }
                Err(e) => {
                    error!("❌ Failed to initialize chain {}: {}", name, e);
                    initialization_errors.push((name.clone(), e));

                    // Mark as unhealthy
                    let mut health = self.chain_health.write().await;
                    if let Some(chain_health) = health.get_mut(name) {
                        chain_health.record_failure();
                    }
                }
            }
        }

        // Report initialization results
        if successfully_initialized == 0 {
            return Err(InjectorError::ConfigError(
                "No chains could be initialized successfully".to_string(),
            ));
        }

        if !initialization_errors.is_empty() {
            warn!(
                "Initialization completed with {} successful chains and {} failures:",
                successfully_initialized,
                initialization_errors.len()
            );
            for (chain, error) in initialization_errors {
                warn!("  - {}: {}", chain, error);
            }
        } else {
            info!(
                "✅ All {} chains initialized successfully",
                successfully_initialized
            );
        }

        Ok(())
    }

    /// Connect to a chain with retry logic
    async fn connect_with_retry(
        &self,
        client: &mut ChainClient,
        chain_name: &str,
        max_retries: u32,
    ) -> Result<(), InjectorError> {
        let mut last_error = None;

        for attempt in 1..=max_retries {
            match client.connect().await {
                Ok(_) => {
                    if attempt > 1 {
                        info!(
                            "Successfully connected to {} after {} attempts",
                            chain_name, attempt
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries {
                        let delay = Duration::from_secs(2_u64.pow(attempt - 1)); // Exponential backoff
                        warn!(
                            "Connection attempt {}/{} failed for {}, retrying in {:?}: {}",
                            attempt,
                            max_retries,
                            chain_name,
                            delay,
                            last_error.as_ref().unwrap()
                        );
                        time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Start listening to all enabled chains with comprehensive monitoring
    pub async fn start(&mut self) -> Result<(), InjectorError> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        info!(
            "🚀 Starting resilient event listener for {} chains",
            self.clients.len()
        );

        // Create log processor with source database - await the future first
        let log_processor =
            LogProcessor::new(self.config.clone(), self.db.clone(), self.source_db.clone()).await;

        // Now wrap in Arc
        let processor_arc = Arc::new(log_processor);

        // Start polling for each chain
        self.start_chain_polling(&processor_arc).await?;

        // Start monitoring systems
        self.start_monitoring_systems().await;

        info!("🎯 All systems started, waiting for shutdown signal...");

        // Wait for shutdown signal
        tokio::select! {
            _ = shutdown_rx.recv() => info!("Received shutdown signal"),
            _ = tokio::signal::ctrl_c() => info!("Received Ctrl+C, shutting down gracefully"),
        }

        Ok(())
    }

    /// Start polling for all chains
    async fn start_chain_polling(
        &mut self,
        log_processor: &Arc<LogProcessor>,
    ) -> Result<(), InjectorError> {
        for (chain_name, client_arc) in &self.clients {
            let chain_name_clone = chain_name.clone();
            let processor_clone = log_processor.clone();
            let health_clone = self.chain_health.clone();

            let mut client_lock = client_arc.lock().await;
            let polling_callback =
                self.create_polling_callback(chain_name_clone, processor_clone, health_clone);

            if let Err(e) = client_lock.start_polling(polling_callback).await {
                error!("❌ Failed to start polling for chain {}: {}", chain_name, e);
                self.mark_chain_unhealthy(chain_name).await;
            } else {
                info!("✅ Started polling for chain: {}", chain_name);
            }
        }
        Ok(())
    }

    /// Create polling callback function
    fn create_polling_callback(
        &self,
        _chain_name: String,
        log_processor: Arc<LogProcessor>,
        chain_health: Arc<RwLock<HashMap<String, ChainHealth>>>,
    ) -> impl Fn(&str, &Block<H256>, &[Log], &[Transaction]) + Send + Sync + 'static {
        move |chain: &str, block: &Block<H256>, logs: &[Log], transactions: &[Transaction]| {
            let block_number = block.number.unwrap_or_default();

            info!(
                "🔍 Chain {}: New block #{} with {} logs, {} transactions",
                chain,
                block_number,
                logs.len(),
                transactions.len()
            );

            // Process asynchronously
            let processor = log_processor.clone();
            let health = chain_health.clone();
            let chain_name = chain.to_string();
            let block_clone = block.clone();
            let logs_clone = logs.to_vec();
            let transactions_clone = transactions.to_vec();

            tokio::spawn(async move {
                Self::process_block_async(
                    &processor,
                    &health,
                    &chain_name,
                    &block_clone,
                    &logs_clone,
                    &transactions_clone,
                )
                .await;
            });
        }
    }

    /// Process block asynchronously with simplified error handling
    async fn process_block_async(
        log_processor: &LogProcessor, // Changed type
        chain_health: &Arc<RwLock<HashMap<String, ChainHealth>>>,
        chain_name: &str,
        block: &Block<H256>,
        logs: &[Log],
        _transactions: &[Transaction],
    ) {
        let block_number = block.number.unwrap_or_default();
        let log_count = logs.len() as u32;

        // Only process blocks that have logs
        if log_count == 0 {
            debug!(
                "📋 Block #{} on {} has no logs - skipping processing",
                block_number, chain_name
            );

            // Still update health status even for empty blocks with timeout protection
            match tokio::time::timeout(Duration::from_secs(1), chain_health.write()).await {
                Ok(mut health) => {
                    if let Some(chain_health) = health.get_mut(chain_name) {
                        chain_health.record_success();
                    }
                }
                Err(_) => {
                    debug!(
                        "⚠️ Timeout updating health status for chain {} - skipping",
                        chain_name
                    );
                }
            }
            return; // Skip processing for blocks with no logs
        }

        // Extract block information
        let block_number_val = block.number.unwrap_or_default().as_u64();
        let block_timestamp_val = block.timestamp.as_u64();
        let block_hash_val = format!(
            "0x{}",
            hex::encode(block.hash.unwrap_or_default().as_bytes())
        );

        // Convert ethers::Log to EthLog with proper validation
        let eth_logs: Vec<crate::models::EthLog> = logs
            .iter()
            .enumerate()
            .filter_map(|(i, log)| {
                // Validate required fields are present
                let transaction_hash = log.transaction_hash?;
                let transaction_index = log.transaction_index?;
                let log_index = log.log_index?;
                
                // Check for zero hash (indicates invalid/pending transaction)
                if transaction_hash.is_zero() {
                    warn!(
                        "⚠️ Skipping log {}/{} with zero transaction hash in block #{}", 
                        i + 1, logs.len(), block_number_val
                    );
                    return None;
                }
                
                // Check for empty topics (invalid log)
                if log.topics.is_empty() {
                    warn!(
                        "⚠️ Skipping log {}/{} with no topics in block #{}", 
                        i + 1, logs.len(), block_number_val
                    );
                    return None;
                }
                
                // Check for zero address (invalid contract)
                if log.address.is_zero() {
                    warn!(
                        "⚠️ Skipping log {}/{} with zero address in block #{}", 
                        i + 1, logs.len(), block_number_val
                    );
                    return None;
                }
                
                debug!(
                    "✅ Converting valid log {}/{}: tx_hash={:?}, tx_index={}, log_index={}, address={:?}",
                    i + 1, logs.len(), transaction_hash, transaction_index, log_index, log.address
                );
                
                Some(crate::models::EthLog {
                    address: format!("0x{:040x}", log.address), // Use proper formatting
                    topics: log
                        .topics
                        .iter()
                        .map(|topic| format!("0x{:064x}", topic)) // Use proper formatting
                        .collect(),
                    data: format!("0x{}", hex::encode(&log.data)),
                    block_number: format!("0x{block_number_val:x}"),
                    transaction_hash: format!("0x{:064x}", transaction_hash),
                    transaction_index: format!("0x{:x}", transaction_index.as_u32()),
                    log_index: format!("0x{:x}", log_index.as_u32()),
                    removed: log.removed,
                })
            })
            .collect();
            
        if eth_logs.len() != logs.len() {
            info!(
                "📊 Filtered logs for block #{}: {} valid out of {} total ({} invalid logs skipped)",
                block_number_val,
                eth_logs.len(),
                logs.len(),
                logs.len() - eth_logs.len()
            );
        }

        // Process and save filtered logs
        match log_processor
            .process_logs(
                chain_name,
                block_number_val,
                block_timestamp_val,
                &block_hash_val,
                &eth_logs,
            )
            .await
        {
            Ok(()) => {
                info!(
                    "✅ Processed and saved filtered events for block #{}",
                    block_number_val
                );
            }
            Err(e) => {
                error!(
                    "❌ Failed to process logs for block #{}: {}",
                    block_number, e
                );
                return;
            }
        }

        // Update health status with timeout protection
        match tokio::time::timeout(Duration::from_secs(1), chain_health.write()).await {
            Ok(mut health) => {
                if let Some(chain_health) = health.get_mut(chain_name) {
                    chain_health.record_success();
                }
            }
            Err(_) => {
                debug!(
                    "⚠️ Timeout updating health status for chain {} - skipping",
                    chain_name
                );
            }
        }
    }

    /// Start all monitoring systems
    async fn start_monitoring_systems(&mut self) {
        self.start_health_monitor().await;
        self.start_recovery_manager().await;
    }

    /// Mark a chain as unhealthy
    async fn mark_chain_unhealthy(&self, chain_name: &str) {
        match tokio::time::timeout(Duration::from_secs(1), self.chain_health.write()).await {
            Ok(mut health) => {
                if let Some(chain_health) = health.get_mut(chain_name) {
                    chain_health.record_failure();
                }
            }
            Err(_) => {
                debug!(
                    "⚠️ Timeout marking chain {} as unhealthy - skipping",
                    chain_name
                );
            }
        }
    }

    /// Start health monitoring for all chains
    async fn start_health_monitor(&mut self) {
        let chain_health = self.chain_health.clone();
        let health_check_interval = Duration::from_millis(self.config.app.health_check_interval_ms);

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(health_check_interval);

            loop {
                interval.tick().await;

                let health = chain_health.read().await;
                let mut unhealthy_chains = Vec::new();
                let mut stale_chains = Vec::new();

                for (name, chain_health) in health.iter() {
                    if !chain_health.is_healthy {
                        unhealthy_chains.push(name.clone());
                    }

                    if chain_health.is_stale(Duration::from_secs(300)) {
                        // 5 minutes
                        stale_chains.push(name.clone());
                    }
                }

                if !unhealthy_chains.is_empty() {
                    warn!("🏥 Unhealthy chains detected: {:?}", unhealthy_chains);
                }

                if !stale_chains.is_empty() {
                    warn!(
                        "⏰ Stale chains detected (no activity in 5+ minutes): {:?}",
                        stale_chains
                    );
                }

                // Log health summary every 10 minutes
                if !health.is_empty() {
                    let healthy_count = health.values().filter(|h| h.is_healthy).count();
                    debug!(
                        "Health check: {}/{} chains healthy",
                        healthy_count,
                        health.len()
                    );
                }
            }
        });

        self.health_monitor_handle = Some(handle);
    }

    /// Start recovery manager to attempt reconnection of failed chains
    async fn start_recovery_manager(&mut self) {
        let clients = self.clients.clone();
        let chain_health = self.chain_health.clone();
        // Prefix with underscore to indicate intentionally unused variable
        let _topics = self.config.topics.clone();

        let handle = tokio::spawn(async move {
            let mut recovery_interval = time::interval(Duration::from_secs(60)); // Check every minute

            loop {
                recovery_interval.tick().await;

                let health = chain_health.read().await;
                let failed_chains: Vec<String> = health
                    .iter()
                    .filter(|(_, h)| !h.is_healthy && h.consecutive_failures >= 5)
                    .map(|(name, _)| name.clone())
                    .collect();
                drop(health);

                for chain_name in failed_chains {
                    info!("🔄 Attempting recovery for failed chain: {}", chain_name);

                    if let Some(client_arc) = clients.get(&chain_name) {
                        let mut client = client_arc.lock().await;

                        // Attempt reconnection
                        match client.connect().await {
                            Ok(_) => {
                                info!("✅ Successfully recovered chain: {}", chain_name);

                                // Reset health status with timeout protection
                                match tokio::time::timeout(
                                    Duration::from_secs(1),
                                    chain_health.write(),
                                )
                                .await
                                {
                                    Ok(mut health) => {
                                        if let Some(chain_health) = health.get_mut(&chain_name) {
                                            chain_health.record_success();
                                        }
                                    }
                                    Err(_) => {
                                        debug!("⚠️ Timeout updating health status for recovered chain {} - skipping", chain_name);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("❌ Recovery failed for chain {}: {}", chain_name, e);

                                // Record failure with timeout protection
                                match tokio::time::timeout(
                                    Duration::from_secs(1),
                                    chain_health.write(),
                                )
                                .await
                                {
                                    Ok(mut health) => {
                                        if let Some(chain_health) = health.get_mut(&chain_name) {
                                            chain_health.record_failure();
                                        }
                                    }
                                    Err(_) => {
                                        debug!("⚠️ Timeout updating health status for failed recovery {} - skipping", chain_name);
                                    }
                                }
                            }
                        }
                    }

                    // Avoid overwhelming with recovery attempts
                    time::sleep(Duration::from_secs(10)).await;
                }
            }
        });

        self.recovery_manager_handle = Some(handle);
    }

    /// Get comprehensive health status for all chains
    pub async fn get_health_status(&self) -> Vec<ChainHealth> {
        let health = self.chain_health.read().await;
        health.values().cloned().collect()
    }

    /// Get current topic statistics
    #[allow(dead_code)]
    pub async fn get_topic_stats(&self) -> Vec<TopicStats> {
        let mut result = Vec::new();

        for client in self.clients.values() {
            let client_lock = client.lock().await;
            let stats = client_lock.get_topic_stats();
            result.extend(stats);
        }

        result
    }

    /// Force reconnection of a specific chain
    #[allow(dead_code)]
    pub async fn force_reconnect(&self, chain_name: &str) -> Result<(), InjectorError> {
        if let Some(client_arc) = self.clients.get(chain_name) {
            let mut client = client_arc.lock().await;

            info!("🔄 Force reconnecting chain: {}", chain_name);

            match client.connect().await {
                Ok(_) => {
                    info!("✅ Force reconnection successful for: {}", chain_name);

                    // Update health status with timeout protection
                    match tokio::time::timeout(Duration::from_secs(1), self.chain_health.write())
                        .await
                    {
                        Ok(mut health) => {
                            if let Some(chain_health) = health.get_mut(chain_name) {
                                chain_health.record_success();
                            }
                        }
                        Err(_) => {
                            debug!("⚠️ Timeout updating health status for force reconnect {} - skipping", chain_name);
                        }
                    }

                    Ok(())
                }
                Err(e) => {
                    error!("❌ Force reconnection failed for {}: {}", chain_name, e);

                    // Record failure with timeout protection
                    match tokio::time::timeout(Duration::from_secs(1), self.chain_health.write())
                        .await
                    {
                        Ok(mut health) => {
                            if let Some(chain_health) = health.get_mut(chain_name) {
                                chain_health.record_failure();
                            }
                        }
                        Err(_) => {
                            debug!("⚠️ Timeout updating health status for failed force reconnect {} - skipping", chain_name);
                        }
                    }

                    Err(e)
                }
            }
        } else {
            Err(InjectorError::ConfigError(format!(
                "Chain {chain_name} not found"
            )))
        }
    }

    /// Shutdown the event listener gracefully
    pub async fn shutdown(&mut self) {
        info!("🛑 Initiating graceful shutdown...");

        // Stop main polling
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }

        // Stop health monitor
        if let Some(handle) = self.health_monitor_handle.take() {
            handle.abort();
            info!("Health monitor stopped");
        }

        // Stop recovery manager
        if let Some(handle) = self.recovery_manager_handle.take() {
            handle.abort();
            info!("Recovery manager stopped");
        }

        // Final health report
        let health_status = self.get_health_status().await;
        let healthy_count = health_status.iter().filter(|h| h.is_healthy).count();

        info!(
            "📊 Final health report: {}/{} chains healthy at shutdown",
            healthy_count,
            health_status.len()
        );

        for chain in health_status {
            info!(
                "  - {}: {} (successes: {}, failures: {})",
                chain.name,
                if chain.is_healthy {
                    "✅ Healthy"
                } else {
                    "❌ Unhealthy"
                },
                chain.total_successes,
                chain.total_failures
            );
        }

        info!("✅ Event listener shutdown completed");
    }
}
