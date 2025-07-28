use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ethers::prelude::*;
use ethers::providers::Http;
use ethers::types::{Filter, Log, H256};
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::ChainConfig;
use crate::error::InjectorError;

// ERC20 ABI for balance fetching
abigen!(
    ERC20Token,
    r#"[
        {
            "constant": true,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function"
        }
    ]"#,
);

/// Rate limiter for RPC calls
#[derive(Debug, Clone)]
pub struct RateLimiter {
    /// Maximum number of concurrent requests
    _max_concurrent: usize,
    /// Semaphore to control concurrency
    semaphore: Arc<Semaphore>,
    /// Minimum delay between requests
    min_request_interval: Duration,
    /// Last request timestamp
    last_request: Arc<tokio::sync::Mutex<Instant>>,
    /// Current backoff duration (exponential)
    current_backoff: Arc<tokio::sync::Mutex<Duration>>,
    /// Base backoff duration
    base_backoff: Duration,
    /// Maximum backoff duration
    max_backoff: Duration,
}

impl RateLimiter {
    pub fn new(max_concurrent: usize, requests_per_second: f64) -> Self {
        let min_request_interval = Duration::from_secs_f64(1.0 / requests_per_second);

        Self {
            _max_concurrent: max_concurrent,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            min_request_interval,
            last_request: Arc::new(tokio::sync::Mutex::new(Instant::now())),
            current_backoff: Arc::new(tokio::sync::Mutex::new(Duration::from_millis(100))),
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
        }
    }

    /// Acquire a permit for making an RPC call with rate limiting
    pub async fn acquire(&self) -> RateLimitGuard {
        // Wait for available slot
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed");

        // Enforce minimum interval between requests
        let mut last_request = self.last_request.lock().await;
        let elapsed = last_request.elapsed();

        if elapsed < self.min_request_interval {
            let sleep_duration = self.min_request_interval - elapsed;
            debug!("Rate limiting: sleeping for {:?}", sleep_duration);
            time::sleep(sleep_duration).await;
        }

        *last_request = Instant::now();
        drop(last_request);

        RateLimitGuard {
            _permit: permit,
            _rate_limiter: self,
        }
    }

    /// Apply exponential backoff after a rate limit error
    pub async fn apply_backoff(&self) {
        let mut current_backoff = self.current_backoff.lock().await;
        let backoff_duration = *current_backoff;

        warn!("Applying exponential backoff: {:?}", backoff_duration);
        time::sleep(backoff_duration).await;

        // Increase backoff for next time (exponential)
        *current_backoff = std::cmp::min(*current_backoff * 2, self.max_backoff);
    }

    /// Reset backoff after successful requests
    pub async fn reset_backoff(&self) {
        let mut current_backoff = self.current_backoff.lock().await;
        *current_backoff = self.base_backoff;
    }

    /// Check if error indicates rate limiting
    pub fn is_rate_limit_error(&self, error: &str) -> bool {
        let error_lower = error.to_lowercase();
        error_lower.contains("rate limit")
            || error_lower.contains("too many requests")
            || error_lower.contains("429")
            || error_lower.contains("quota exceeded")
            || error_lower.contains("request limit")
            || error_lower.contains("your app has exceeded its")
            || error_lower.contains("exceeded the maximum number of requests")
            || error_lower.contains("timeout")
            || error_lower.contains("size is larger")
            || error_lower.contains("reducing your block range")
    }
}

/// Guard that releases the rate limit permit when dropped
pub struct RateLimitGuard<'a> {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _rate_limiter: &'a RateLimiter,
}

/// Circuit breaker states
#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing, reject requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker for RPC connections
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: Arc<tokio::sync::Mutex<CircuitState>>,
    failure_count: Arc<tokio::sync::Mutex<u32>>,
    last_failure_time: Arc<tokio::sync::Mutex<Option<Instant>>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    half_open_max_calls: u32,
    half_open_calls: Arc<tokio::sync::Mutex<u32>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(CircuitState::Closed)),
            failure_count: Arc::new(tokio::sync::Mutex::new(0)),
            last_failure_time: Arc::new(tokio::sync::Mutex::new(None)),
            failure_threshold,
            recovery_timeout,
            half_open_max_calls: 3,
            half_open_calls: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    /// Check if request should be allowed
    pub async fn can_execute(&self) -> bool {
        let mut state = self.state.lock().await;

        match *state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                let last_failure = self.last_failure_time.lock().await;
                if let Some(last_failure_time) = *last_failure {
                    if last_failure_time.elapsed() >= self.recovery_timeout {
                        *state = CircuitState::HalfOpen;
                        let mut half_open_calls = self.half_open_calls.lock().await;
                        *half_open_calls = 0;
                        info!("Circuit breaker transitioning to half-open state");
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => {
                let mut half_open_calls = self.half_open_calls.lock().await;
                if *half_open_calls < self.half_open_max_calls {
                    *half_open_calls += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record successful execution
    pub async fn record_success(&self) {
        let mut state = self.state.lock().await;
        let mut failure_count = self.failure_count.lock().await;

        *failure_count = 0;

        if *state == CircuitState::HalfOpen {
            *state = CircuitState::Closed;
            info!("Circuit breaker closed - service recovered");
        }
    }

    /// Record failed execution
    pub async fn record_failure(&self) {
        let mut state = self.state.lock().await;
        let mut failure_count = self.failure_count.lock().await;
        let mut last_failure_time = self.last_failure_time.lock().await;

        *failure_count += 1;
        *last_failure_time = Some(Instant::now());

        if *failure_count >= self.failure_threshold && *state != CircuitState::Open {
            *state = CircuitState::Open;
            error!(
                "Circuit breaker opened - too many failures ({})",
                *failure_count
            );
        }
    }
}

/// Struct for tracking topic statistics
#[derive(Debug, Clone)]
pub struct TopicStats {
    pub chain_name: String,
    pub topic: String,
    pub log_count: usize,
}

/// Trait for chain client functionality
#[async_trait::async_trait]
pub trait ChainClientTrait {
    async fn connect(&mut self) -> Result<(), InjectorError>;
    async fn get_latest_block_number(&self) -> Result<u64, InjectorError>;
    async fn get_block(&self, block_number: u64) -> Result<Block<H256>, InjectorError>;
    async fn get_block_logs(&self, block_number: u64) -> Result<Vec<Log>, InjectorError>;
}

#[derive(Clone)]
pub struct ChainClient {
    pub name: String,
    pub config: ChainConfig,
    pub chain_id: u64,
    provider: Option<Arc<Provider<Http>>>, // Use Arc for shared ownership
    _topic_stats: HashMap<String, usize>,
    topic_filters: Vec<H256>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    /// Track consecutive successful requests for health monitoring
    consecutive_successes: Arc<tokio::sync::Mutex<u32>>,
}

impl std::fmt::Debug for ChainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainClient")
            .field("name", &self.name)
            .field("config", &self.config)
            .field("chain_id", &self.chain_id)
            .field("provider", &self.provider.is_some())
            .field("_topic_stats", &self._topic_stats)
            .field("topic_filters", &self.topic_filters)
            .field("rate_limiter", &self.rate_limiter)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("consecutive_successes", &"<Mutex<u32>>")
            .finish()
    }
}

impl ChainClient {
    pub fn new(name: String, config: ChainConfig) -> Self {
        // Configure rate limiting based on provider
        let (max_concurrent, requests_per_second) = if config.rpc_url.contains("alchemy") {
            (20, 300.0) // Alchemy: Higher limits for paid plans
        } else if config.rpc_url.contains("infura") {
            (8, 10.0) // Infura: ~10 RPS, 8 concurrent
        } else {
            (5, 5.0) // Conservative defaults
        };

        Self {
            name,
            config,
            chain_id: 0,
            provider: None,
            _topic_stats: HashMap::new(),
            topic_filters: Vec::new(),
            rate_limiter: RateLimiter::new(max_concurrent, requests_per_second),
            circuit_breaker: CircuitBreaker::new(5, Duration::from_secs(30)),
            consecutive_successes: Arc::new(tokio::sync::Mutex::new(0)),
        }
    }

    pub fn set_topic_filters(&mut self, topics: Vec<String>) {
        self.topic_filters = topics
            .iter()
            .filter_map(|t| match H256::from_str(t) {
                Ok(hash) => Some(hash),
                Err(e) => {
                    warn!("Invalid topic filter {}: {}", t, e);
                    None
                }
            })
            .collect();

        info!(
            "Set {} topic filters for chain {}",
            self.topic_filters.len(),
            self.name
        );
    }

    pub fn get_topic_stats(&self) -> Vec<TopicStats> {
        self._topic_stats
            .iter()
            .map(|(topic, count)| TopicStats {
                chain_name: self.name.clone(),
                topic: topic.clone(),
                log_count: *count,
            })
            .collect()
    }

    pub fn reset_stats(&mut self) {
        self._topic_stats.clear();
    }

    /// Execute RPC call with rate limiting, circuit breaker, and retry logic
    async fn execute_rpc_call<T, F, Fut>(
        &self,
        operation_name: &str,
        operation: F,
    ) -> Result<T, InjectorError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, ProviderError>> + Send,
        T: Send,
    {
        const MAX_RETRIES: u32 = 5;
        let mut retry_count = 0;

        loop {
            // Check circuit breaker
            if !self.circuit_breaker.can_execute().await {
                return Err(InjectorError::NetworkError(format!(
                    "Circuit breaker open for chain {}",
                    self.name
                )));
            }

            // Acquire rate limit permit with timeout to prevent deadlock
            let _guard =
                match tokio::time::timeout(Duration::from_secs(10), self.rate_limiter.acquire())
                    .await
                {
                    Ok(guard) => guard,
                    Err(_) => {
                        return Err(InjectorError::NetworkError(format!(
                            "Timeout acquiring rate limit permit for chain {}",
                            self.name
                        )));
                    }
                };

            match operation().await {
                Ok(result) => {
                    // Success: record it and reset backoffs
                    self.circuit_breaker.record_success().await;
                    self.rate_limiter.reset_backoff().await;

                    // Update success counter with timeout protection
                    if let Ok(mut successes) = tokio::time::timeout(
                        Duration::from_secs(1),
                        self.consecutive_successes.lock(),
                    )
                    .await
                    {
                        *successes += 1;
                        if *successes % 100 == 0 {
                            debug!(
                                "Chain {} health: {} consecutive successful requests",
                                self.name, *successes
                            );
                        }
                    }

                    return Ok(result);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    retry_count += 1;

                    // Reset success counter with timeout protection
                    if let Ok(mut successes) = tokio::time::timeout(
                        Duration::from_secs(1),
                        self.consecutive_successes.lock(),
                    )
                    .await
                    {
                        *successes = 0;
                    }

                    // Check if it's a rate limit error
                    if self.rate_limiter.is_rate_limit_error(&error_msg) {
                        warn!("Rate limit detected for {}: {}", operation_name, error_msg);
                        self.rate_limiter.apply_backoff().await;

                        if retry_count <= MAX_RETRIES {
                            continue;
                        }
                    }

                    // Record failure in circuit breaker
                    self.circuit_breaker.record_failure().await;

                    // Check if we should retry
                    if retry_count <= MAX_RETRIES {
                        let is_retryable = self.is_retryable_error(&error_msg);

                        if is_retryable {
                            let delay =
                                Duration::from_millis(100 * u64::pow(2, retry_count.min(6)));
                            warn!(
                                "Retrying {} for chain {} in {:?} (attempt {}/{}): {}",
                                operation_name,
                                self.name,
                                delay,
                                retry_count,
                                MAX_RETRIES,
                                error_msg
                            );
                            time::sleep(delay).await;
                            continue;
                        }
                    }

                    // Final failure
                    error!(
                        "Failed {} for chain {} after {} retries: {}",
                        operation_name, self.name, retry_count, error_msg
                    );
                    return Err(InjectorError::NetworkError(error_msg));
                }
            }
        }
    }

    /// Determine if an error is retryable
    fn is_retryable_error(&self, error: &str) -> bool {
        let error_lower = error.to_lowercase();

        // Retryable errors
        error_lower.contains("timeout")
            || error_lower.contains("connection")
            || error_lower.contains("network")
            || error_lower.contains("temporary")
            || error_lower.contains("rate limit")
            || error_lower.contains("too many requests")
            || error_lower.contains("service unavailable")
            || error_lower.contains("bad gateway")
            || error_lower.contains("gateway timeout")
            || error_lower.contains("internal server error")
    }

    pub async fn start_polling(
        &mut self,
        callback: impl Fn(&str, &Block<H256>, &[Log], &[Transaction]) + Send + Sync + 'static,
    ) -> Result<(), InjectorError> {
        // Ensure client is connected
        if self.provider.is_none() {
            self.connect().await?;
        }

        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        // Clone all necessary data to avoid capturing &self
        let name = self.name.clone();
        let poll_interval = self.config.poll_interval_seconds;
        let topic_filters = self.topic_filters.clone();

        // Create owned copies of the rate limiter and circuit breaker references
        let rate_limiter = self.rate_limiter.clone();
        let circuit_breaker = self.circuit_breaker.clone();

        // Create unbounded channel for non-blocking callback processing
        let (callback_tx, mut callback_rx) = mpsc::unbounded_channel::<BlockProcessingData>();

        // Spawn callback processor task
        let callback_processor_name = name.clone();
        tokio::spawn(async move {
            info!(
                "Starting callback processor for chain {}",
                callback_processor_name
            );

            while let Some(data) = callback_rx.recv().await {
                // Process callback in separate task to prevent blocking
                let start_time = Instant::now();

                callback(
                    &data.chain_name,
                    &data.block,
                    &data.logs,
                    &data.transactions,
                );

                let processing_time = start_time.elapsed();
                if processing_time > Duration::from_millis(100) {
                    debug!(
                        "Callback processing for block #{} on {} took {:?}",
                        data.block.number.unwrap_or_default(),
                        data.chain_name,
                        processing_time
                    );
                }
            }

            warn!(
                "Callback processor for chain {} shutting down",
                callback_processor_name
            );
        });

        // Set up initial state for polling
        let mut last_block_number: Option<U64> = None;
        let mut consecutive_failures = 0u32;
        const MAX_CONSECUTIVE_FAILURES: u32 = 10;

        tokio::spawn(async move {
            info!("Starting immediate block polling for chain {} with interval of {} seconds (no confirmations)",
                  name, poll_interval);

            loop {
                // Create a rate-limited RPC call
                let get_block_number = || async { provider.get_block_number().await };

                match Self::execute_rpc_with_protection(
                    &name,
                    &rate_limiter,
                    &circuit_breaker,
                    "get_block_number",
                    get_block_number,
                )
                .await
                {
                    Ok(current_block_number) => {
                        consecutive_failures = 0; // Reset failure counter

                        // Process latest blocks immediately (no confirmations wait)
                        let blocks_to_process = match last_block_number {
                            Some(last_number) => {
                                if current_block_number > last_number {
                                    let start_block = last_number + 1;
                                    let end_block = current_block_number;

                                    // Limit the range to prevent overwhelming the system
                                    let max_blocks_per_batch = 10;
                                    let actual_end = if (end_block - start_block + 1).as_u64()
                                        > max_blocks_per_batch
                                    {
                                        start_block + max_blocks_per_batch - 1
                                    } else {
                                        end_block
                                    };

                                    let blocks: Vec<u64> =
                                        (start_block.as_u64()..=actual_end.as_u64()).collect();

                                    if blocks.len() > 1 {
                                        info!(
                                            "Processing {} new blocks ({} to {}) for chain {}",
                                            blocks.len(),
                                            start_block,
                                            actual_end,
                                            name
                                        );
                                    }

                                    blocks
                                } else {
                                    Vec::new() // No new blocks
                                }
                            }
                            None => {
                                // First run, process only the latest block
                                info!(
                                    "First run for chain {}, processing latest block #{}",
                                    name, current_block_number
                                );
                                vec![current_block_number.as_u64()]
                            }
                        };

                        // Process each block immediately with better error handling
                        let mut processed_any = false;
                        for block_number in blocks_to_process.iter() {
                            match Self::process_single_block_with_protection(
                                &provider,
                                &name,
                                *block_number,
                                &topic_filters,
                                &callback_tx,
                                &rate_limiter,
                                &circuit_breaker,
                            )
                            .await
                            {
                                Ok(processed) => {
                                    if processed {
                                        processed_any = true;
                                    }
                                    // Success - continue to next block
                                }
                                Err(e) => {
                                    warn!("⚠️ Failed to process block #{} on {}: {} - continuing with next block", 
                                          block_number, name, e);
                                    // Continue with next block rather than stopping entirely
                                }
                            }
                        }

                        // Update last seen block number only if we processed blocks successfully
                        if let Some(&max_block) = blocks_to_process.iter().max() {
                            last_block_number = Some(U64::from(max_block));
                            if processed_any {
                                debug!(
                                    "✅ Updated last processed block to #{} for chain {}",
                                    max_block, name
                                );
                            }
                        } else if last_block_number.is_none() {
                            last_block_number = Some(current_block_number);
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        error!(
                            "Failed to get latest block number for {} (failure {}/{}): {}",
                            name, consecutive_failures, MAX_CONSECUTIVE_FAILURES, e
                        );

                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            error!("Chain {} exceeded maximum consecutive failures, entering extended backoff", name);
                            time::sleep(Duration::from_secs(300)).await; // 5 minute backoff
                            consecutive_failures = 0; // Reset after extended backoff
                        }
                    }
                }

                // Sleep until next poll
                time::sleep(Duration::from_secs(poll_interval)).await;
            }
        });

        Ok(())
    }

    /// Execute RPC call with comprehensive protection
    async fn execute_rpc_with_protection<T, F, Fut>(
        chain_name: &str,
        rate_limiter: &RateLimiter,
        circuit_breaker: &CircuitBreaker,
        operation_name: &str,
        operation: F,
    ) -> Result<T, InjectorError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, ProviderError>> + Send,
        T: Send,
    {
        const MAX_RETRIES: u32 = 5;
        let mut retry_count = 0;

        loop {
            // Check circuit breaker
            if !circuit_breaker.can_execute().await {
                return Err(InjectorError::NetworkError(format!(
                    "Circuit breaker open for chain {chain_name}"
                )));
            }

            // Acquire rate limit permit
            let _guard = rate_limiter.acquire().await;

            match operation().await {
                Ok(result) => {
                    circuit_breaker.record_success().await;
                    rate_limiter.reset_backoff().await;
                    return Ok(result);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    retry_count += 1;

                    // Handle rate limiting
                    if rate_limiter.is_rate_limit_error(&error_msg) {
                        rate_limiter.apply_backoff().await;
                        if retry_count <= MAX_RETRIES {
                            continue;
                        }
                    }

                    circuit_breaker.record_failure().await;

                    if retry_count <= MAX_RETRIES {
                        let delay = Duration::from_millis(100 * u64::pow(2, retry_count.min(6)));
                        warn!(
                            "Retrying {} for {} in {:?} (attempt {}/{})",
                            operation_name, chain_name, delay, retry_count, MAX_RETRIES
                        );
                        time::sleep(delay).await;
                        continue;
                    }

                    return Err(InjectorError::NetworkError(error_msg));
                }
            }
        }
    }

    async fn process_single_block_with_protection(
        provider: &Provider<Http>,
        chain_name: &str,
        block_number: u64,
        topic_filters: &[H256],
        callback_tx: &mpsc::UnboundedSender<BlockProcessingData>,
        rate_limiter: &RateLimiter,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<bool, InjectorError> {
        // Get block with transactions with retry for missing blocks
        let get_block_with_txs = || async { provider.get_block_with_txs(block_number).await };

        let block_with_txs_opt = match Self::execute_rpc_with_protection(
            chain_name,
            rate_limiter,
            circuit_breaker,
            "get_block_with_txs",
            get_block_with_txs,
        )
        .await
        {
            Ok(opt) => opt,
            Err(e) => {
                // Check if it's a "block not found" error which is expected during reorgs
                let error_msg = e.to_string().to_lowercase();
                if error_msg.contains("not found") || error_msg.contains("null") {
                    info!(
                        "🔄 Block #{} not found on {} - likely due to reorg, skipping",
                        block_number, chain_name
                    );
                    return Ok(false); // Not an error, just skip this block
                }
                return Err(e);
            }
        };

        let block_with_txs = match block_with_txs_opt {
            Some(block) => block,
            None => {
                info!(
                    "🔄 Block #{} returned null on {} - likely due to reorg, skipping",
                    block_number, chain_name
                );
                return Ok(false); // Not an error, just skip this block
            }
        };

        // Validate block has required fields
        if block_with_txs.hash.is_none() {
            warn!(
                "⚠️ Block #{} on {} has no hash - skipping",
                block_number, chain_name
            );
            return Ok(false);
        }

        if block_with_txs.number.is_none() {
            warn!(
                "⚠️ Block #{} on {} has no number - skipping",
                block_number, chain_name
            );
            return Ok(false);
        }

        // Extract transactions and create a Block<H256> for consistency
        let transactions = block_with_txs.transactions.clone();
        let block_hash = block_with_txs.hash.unwrap(); // Safe unwrap after validation

        // Create Block<H256> from Block<Transaction> by extracting transaction hashes
        let tx_hashes: Vec<H256> = transactions.iter().map(|tx| tx.hash).collect();
        let block_h256 = Block {
            hash: block_with_txs.hash,
            parent_hash: block_with_txs.parent_hash,
            uncles_hash: block_with_txs.uncles_hash,
            author: block_with_txs.author,
            state_root: block_with_txs.state_root,
            transactions_root: block_with_txs.transactions_root,
            receipts_root: block_with_txs.receipts_root,
            number: block_with_txs.number,
            gas_used: block_with_txs.gas_used,
            gas_limit: block_with_txs.gas_limit,
            extra_data: block_with_txs.extra_data,
            logs_bloom: block_with_txs.logs_bloom,
            timestamp: block_with_txs.timestamp,
            difficulty: block_with_txs.difficulty,
            total_difficulty: block_with_txs.total_difficulty,
            seal_fields: block_with_txs.seal_fields,
            uncles: block_with_txs.uncles,
            transactions: tx_hashes,
            size: block_with_txs.size,
            mix_hash: block_with_txs.mix_hash,
            nonce: block_with_txs.nonce,
            base_fee_per_gas: block_with_txs.base_fee_per_gas,
            withdrawals_root: block_with_txs.withdrawals_root,
            withdrawals: block_with_txs.withdrawals,
            blob_gas_used: block_with_txs.blob_gas_used,
            excess_blob_gas: block_with_txs.excess_blob_gas,
            parent_beacon_block_root: block_with_txs.parent_beacon_block_root,
            other: block_with_txs.other,
        };

        // Get logs with protection and handle missing block gracefully
        let get_logs = || async {
            let filter = Filter::new().at_block_hash(block_hash);
            provider.get_logs(&filter).await
        };

        let logs = match Self::execute_rpc_with_protection(
            chain_name,
            rate_limiter,
            circuit_breaker,
            "get_logs",
            get_logs,
        )
        .await
        {
            Ok(logs) => logs,
            Err(e) => {
                // Check if it's a "block not found" error for logs
                let error_msg = e.to_string().to_lowercase();
                if error_msg.contains("not found") || error_msg.contains("null") {
                    info!(
                        "🔄 Logs for block #{} not found on {} - likely due to reorg, skipping",
                        block_number, chain_name
                    );
                    return Ok(false); // Not an error, just skip this block
                }

                warn!(
                    "⚠️ Failed to get logs for block #{} on {}: {} - continuing with empty logs",
                    block_number, chain_name, e
                );
                Vec::new() // Continue with empty logs rather than failing
            }
        };

        let filtered_logs = if !topic_filters.is_empty() {
            logs.into_iter()
                .filter(|log| {
                    if let Some(first_topic) = log.topics.first() {
                        topic_filters.contains(first_topic)
                    } else {
                        false
                    }
                })
                .collect::<Vec<_>>()
        } else {
            logs
        };

        // Always send to callback queue even with no logs to track the block
        let processing_data = BlockProcessingData {
            chain_name: chain_name.to_string(),
            block: block_h256,
            logs: filtered_logs,
            transactions,
        };

        // Send to callback processor (non-blocking)
        if let Err(e) = callback_tx.send(processing_data) {
            warn!(
                "Failed to send block data to callback processor for chain {}: {}",
                chain_name, e
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Fetch balances for multiple addresses from on-chain in a single batch
    pub async fn fetch_balances_batch(
        &self,
        token: &str,
        addresses: &[&str],
    ) -> Result<HashMap<String, String>, InjectorError> {
        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        info!(
            "🔗 Batch fetching on-chain balances for {} addresses on {} for token {}",
            addresses.len(),
            self.name,
            token
        );

        let token_address = Address::from_str(token).map_err(|e| {
            InjectorError::NetworkError(format!("Invalid token address {token}: {e}"))
        })?;

        let contract = ERC20Token::new(token_address, Arc::new(provider.as_ref().clone()));
        let mut balances = HashMap::new();

        // Batch the RPC calls for better performance
        const BATCH_SIZE: usize = 50; // Adjust based on RPC provider limits

        for chunk in addresses.chunks(BATCH_SIZE) {
            debug!("🔄 Processing batch of {} addresses", chunk.len());

            let mut batch_futures = Vec::new();

            for &address in chunk {
                if let Ok(addr) = Address::from_str(address) {
                    let contract_call = contract.balance_of(addr);
                    batch_futures.push(async move {
                        match contract_call.call().await {
                            Ok(balance) => Some((address.to_string(), balance.to_string())),
                            Err(e) => {
                                warn!("Failed to fetch balance for {}: {}", address, e);
                                None
                            }
                        }
                    });
                } else {
                    warn!("Invalid address format: {}", address);
                }
            }

            // Execute batch calls in parallel with rate limiting
            let _guard = self.rate_limiter.acquire().await;
            let batch_results = futures::future::join_all(batch_futures).await;

            for (addr, balance) in batch_results.into_iter().flatten() {
                balances.insert(addr, balance);
            }

            // Small delay between batches to be respectful to RPC providers
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        info!(
            "✅ Successfully fetched {} out of {} on-chain balances",
            balances.len(),
            addresses.len()
        );

        if balances.len() != addresses.len() {
            warn!(
                "⚠️ Some balances could not be fetched: {}/{} successful",
                balances.len(),
                addresses.len()
            );
        }

        Ok(balances)
    }

    /// Fetch single balance from on-chain
    pub async fn fetch_balance(&self, token: &str, address: &str) -> Result<String, InjectorError> {
        let balances = self.fetch_balances_batch(token, &[address]).await?;
        balances
            .get(address)
            .cloned()
            .ok_or_else(|| InjectorError::NetworkError("Failed to fetch balance".to_string()))
    }

    /// Make a raw eth_call to a contract
    pub async fn eth_call(&self, to: &str, data: &str, block: Option<&str>) -> Result<String, InjectorError> {
        let _provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        // Prepare eth_call request
        let call_request = serde_json::json!({
            "to": to,
            "data": data
        });

        let block_param = block.unwrap_or("latest");

        let eth_call = || async {
            // Use the provider's internal HTTP client for raw JSON-RPC
            let client = reqwest::Client::new();
            let request_body = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [call_request, block_param],
                "id": 1
            });

            let response = client
                .post(self.config.rpc_url.as_str())
                .json(&request_body)
                .send()
                .await
                .map_err(|e| ethers::providers::ProviderError::CustomError(e.to_string()))?;

            let json: serde_json::Value = response
                .json()
                .await
                .map_err(|e| ethers::providers::ProviderError::CustomError(e.to_string()))?;

            if let Some(error) = json.get("error") {
                return Err(ethers::providers::ProviderError::CustomError(
                    format!("RPC error: {}", error)
                ));
            }

            json.get("result")
                .and_then(|r| r.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| ethers::providers::ProviderError::CustomError(
                    "Invalid response format".to_string()
                ))
        };

        self.execute_rpc_call("eth_call", eth_call).await
    }

    /// Get ERC20 token balance using raw eth_call
    pub async fn get_token_balance(&self, token_address: &str, holder_address: &str) -> Result<String, InjectorError> {
        // Encode balanceOf(address) call
        // Function selector: balanceOf(address) = 0x70a08231
        // Parameter: address (32 bytes, padded)
        let holder_hex = if holder_address.starts_with("0x") {
            &holder_address[2..]
        } else {
            holder_address
        };
        
        let padded_address = format!("{:0>64}", holder_hex);
        let call_data = format!("0x70a08231{}", padded_address);

        debug!(
            "🔗 Calling balanceOf for token {} holder {}: data={}",
            token_address, holder_address, call_data
        );

        match self.eth_call(token_address, &call_data, None).await {
            Ok(result) => {
                // Convert hex result to decimal string
                if result.starts_with("0x") && result.len() > 2 {
                    match u128::from_str_radix(&result[2..], 16) {
                        Ok(balance) => {
                            debug!(
                                "✅ Token balance for {} on {}: {}",
                                holder_address, token_address, balance
                            );
                            Ok(balance.to_string())
                        }
                        Err(e) => {
                            warn!("Failed to parse balance result '{}': {}", result, e);
                            Ok("0".to_string())
                        }
                    }
                } else {
                    warn!("Invalid balance result format: {}", result);
                    Ok("0".to_string())
                }
            }
            Err(e) => {
                warn!("Failed to get token balance for {} on {}: {}", holder_address, token_address, e);
                Ok("0".to_string()) // Return 0 on error rather than failing
            }
        }
    }

    /// Get voting power using raw eth_call for governance tokens
    pub async fn get_voting_power(&self, token_address: &str, holder_address: &str) -> Result<String, InjectorError> {
        // Encode getVotes(address) call
        // Function selector: getVotes(address) = 0x9ab24eb0
        // Parameter: address (32 bytes, padded)
        let holder_hex = if holder_address.starts_with("0x") {
            &holder_address[2..]
        } else {
            holder_address
        };
        
        let padded_address = format!("{:0>64}", holder_hex);
        let call_data = format!("0x9ab24eb0{}", padded_address);

        debug!(
            "🗳️ Calling getVotes for token {} holder {}: data={}",
            token_address, holder_address, call_data
        );

        match self.eth_call(token_address, &call_data, None).await {
            Ok(result) => {
                // Convert hex result to decimal string
                if result.starts_with("0x") && result.len() > 2 {
                    match u128::from_str_radix(&result[2..], 16) {
                        Ok(votes) => {
                            debug!(
                                "✅ Voting power for {} on {}: {}",
                                holder_address, token_address, votes
                            );
                            Ok(votes.to_string())
                        }
                        Err(e) => {
                            warn!("Failed to parse votes result '{}': {}", result, e);
                            Ok("0".to_string())
                        }
                    }
                } else {
                    warn!("Invalid votes result format: {}", result);
                    Ok("0".to_string())
                }
            }
            Err(e) => {
                warn!("Failed to get voting power for {} on {}: {}", holder_address, token_address, e);
                Ok("0".to_string()) // Return 0 on error rather than failing
            }
        }
    }
}

/// Data structure for callback queue
#[derive(Debug, Clone)]
pub struct BlockProcessingData {
    pub chain_name: String,
    pub block: Block<H256>,
    pub logs: Vec<Log>,
    pub transactions: Vec<Transaction>,
}

// Implement the trait for ChainClient with enhanced error handling
#[async_trait::async_trait]
impl ChainClientTrait for ChainClient {
    async fn connect(&mut self) -> Result<(), InjectorError> {
        let create_provider = || async {
            let provider = Provider::<Http>::try_from(self.config.rpc_url.clone())
                .map_err(|e| ProviderError::CustomError(e.to_string()))?;
            Ok(provider)
        };

        let provider = self
            .execute_rpc_call("create_provider", create_provider)
            .await
            .map_err(|e| {
                InjectorError::ConnectionError(format!("Failed to create provider: {e}"))
            })?;

        // Get chain ID with protection
        let get_chain_id = || async { provider.get_chainid().await };

        match self.execute_rpc_call("get_chain_id", get_chain_id).await {
            Ok(chain_id) => {
                self.chain_id = chain_id.as_u64();
                info!("Connected to chain {} with ID {}", self.name, self.chain_id);
            }
            Err(e) => {
                return Err(InjectorError::ConnectionError(format!(
                    "Failed to get chain ID for {}: {}",
                    self.name, e
                )));
            }
        }

        self.provider = Some(Arc::new(provider)); // Wrap in Arc
        Ok(())
    }

    async fn get_latest_block_number(&self) -> Result<u64, InjectorError> {
        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        let get_block_number = || async { provider.get_block_number().await };

        let block_number = self
            .execute_rpc_call("get_block_number", get_block_number)
            .await?;
        Ok(block_number.as_u64())
    }

    async fn get_block(&self, block_number: u64) -> Result<Block<H256>, InjectorError> {
        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        let get_block = || async { provider.get_block(block_number).await };

        let block_opt = self.execute_rpc_call("get_block", get_block).await?;
        block_opt.ok_or_else(|| {
            InjectorError::UnexpectedError(format!("Block {block_number} not found"))
        })
    }

    async fn get_block_logs(&self, block_number: u64) -> Result<Vec<Log>, InjectorError> {
        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        // Get the block to get its hash
        let block = self.get_block(block_number).await?;
        let block_hash = block
            .hash
            .ok_or_else(|| InjectorError::UnexpectedError("Block hash is missing".to_string()))?;

        let get_logs = || async {
            let filter = Filter::new().at_block_hash(block_hash);
            provider.get_logs(&filter).await
        };

        let logs = self.execute_rpc_call("get_logs", get_logs).await?;
        Ok(logs)
    }
}
