use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ethers::prelude::*;
use ethers::providers::Http;
use ethers::types::{Filter, Log, H256};
use futures::future::join_all;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::ChainConfig;
use crate::error::InjectorError;

pub mod web3_batch_helper;
pub use web3_batch_helper::*;

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

/// Rate limiter for RPC calls with metrics
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
    /// Metrics tracking
    metrics: Arc<tokio::sync::Mutex<RateLimiterMetrics>>,
}

#[derive(Debug, Default)]
pub struct RateLimiterMetrics {
    pub total_requests: u64,
    pub rate_limited_requests: u64,
    pub total_backoff_time_ms: u64,
    pub current_backoff_level: u32,
    pub last_reset_time: Option<Instant>,
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
            metrics: Arc::new(tokio::sync::Mutex::new(RateLimiterMetrics::default())),
        }
    }

    /// Acquire a permit for making an RPC call with rate limiting
    pub async fn acquire(&self) -> RateLimitGuard {
        // Update metrics
        {
            let mut metrics = self.metrics.lock().await;
            metrics.total_requests += 1;
        }

        // Wait for available slot
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed");

        // Enforce minimum interval between requests (reduced enforcement)
        let mut last_request = self.last_request.lock().await;
        let elapsed = last_request.elapsed();

        if elapsed < self.min_request_interval {
            let sleep_duration = self.min_request_interval - elapsed;
            // Only sleep if duration is significant (>10ms) to reduce micro-delays
            if sleep_duration > Duration::from_millis(10) {
                debug!("Rate limiting: sleeping for {:?}", sleep_duration);
                time::sleep(sleep_duration).await;
            }
        }

        *last_request = Instant::now();
        drop(last_request);

        RateLimitGuard {
            _permit: permit,
            _rate_limiter: self,
        }
    }

    /// Apply exponential backoff after a rate limit error with jitter
    pub async fn apply_backoff(&self) {
        // Update metrics
        {
            let mut metrics = self.metrics.lock().await;
            metrics.rate_limited_requests += 1;
            metrics.current_backoff_level += 1;
        }

        let mut current_backoff = self.current_backoff.lock().await;
        let base_duration = *current_backoff;
        
        // Add simple jitter using current time to prevent thundering herd problem
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));
        let jitter_factor = (now.as_nanos() % 100) as f64 / 1000.0; // 0-0.1 jitter factor
        let jitter_ms = (base_duration.as_millis() as f64 * jitter_factor) as u64;
        let backoff_duration = base_duration + Duration::from_millis(jitter_ms);

        info!("⏳ Applying exponential backoff: {:?} (with {}ms jitter)", base_duration, jitter_ms);
        
        // Update metrics with actual backoff time
        {
            let mut metrics = self.metrics.lock().await;
            metrics.total_backoff_time_ms += backoff_duration.as_millis() as u64;
        }

        time::sleep(backoff_duration).await;

        // More conservative backoff increase - was 2x, now 1.5x
        let next_backoff = Duration::from_millis((base_duration.as_millis() as f64 * 1.5) as u64);
        *current_backoff = std::cmp::min(next_backoff, self.max_backoff);
    }

    /// Reset backoff after successful requests
    pub async fn reset_backoff(&self) {
        let mut current_backoff = self.current_backoff.lock().await;
        *current_backoff = self.base_backoff;

        // Reset metrics backoff level
        {
            let mut metrics = self.metrics.lock().await;
            metrics.current_backoff_level = 0;
            metrics.last_reset_time = Some(Instant::now());
        }
    }

    /// Get rate limiter metrics for monitoring
    pub async fn get_metrics(&self) -> RateLimiterMetrics {
        let metrics = self.metrics.lock().await;
        RateLimiterMetrics {
            total_requests: metrics.total_requests,
            rate_limited_requests: metrics.rate_limited_requests,
            total_backoff_time_ms: metrics.total_backoff_time_ms,
            current_backoff_level: metrics.current_backoff_level,
            last_reset_time: metrics.last_reset_time,
        }
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
    provider: Option<Arc<Provider<Http>>>,
    topic_filters: Vec<H256>,
    rate_limiter: RateLimiter,
    circuit_breaker: CircuitBreaker,
    batch_helper: Web3BatchHelper,
}

impl std::fmt::Debug for ChainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainClient")
            .field("name", &self.name)
            .field("chain_id", &self.chain_id)
            .field("connected", &self.provider.is_some())
            .finish()
    }
}

impl ChainClient {
    pub fn new(name: String, config: ChainConfig) -> Self {
        // Configure rate limiting based on provider (more aggressive defaults)
        let (max_concurrent, requests_per_second) = if config.rpc_url.contains("alchemy") {
            (50, 500.0) // Alchemy: Higher limits for paid plans
        } else if config.rpc_url.contains("infura") {
            (20, 50.0) // Infura: Increased from conservative limits
        } else {
            (15, 25.0) // More aggressive defaults (was 5, 5.0)
        };

        Self {
            name,
            config,
            chain_id: 0,
            provider: None,
            topic_filters: Vec::new(),
            rate_limiter: RateLimiter::new(max_concurrent, requests_per_second),
            circuit_breaker: CircuitBreaker::new(5, Duration::from_secs(30)),
            batch_helper: Web3BatchHelper::new(),
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

            // Acquire rate limit permit with reduced timeout
            let _guard =
                match tokio::time::timeout(Duration::from_secs(2), self.rate_limiter.acquire())
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

                    // Success recorded in circuit breaker and rate limiter

                    return Ok(result);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    retry_count += 1;

                    // Failure recorded in circuit breaker

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

                        // Batch process all blocks in the polling period
                        if !blocks_to_process.is_empty() {
                            match Self::process_block_batch_with_protection(
                                &provider,
                                &name,
                                &blocks_to_process,
                                &topic_filters,
                                &callback_tx,
                                &rate_limiter,
                                &circuit_breaker,
                            )
                            .await
                            {
                                Ok(processed_count) => {
                                    if processed_count > 0 {
                                        info!(
                                            "✅ Batch processed {} blocks with events for {}",
                                            processed_count, name
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!("⚠️ Failed to process block batch on {}: {}", name, e);
                                }
                            }
                        }

                        // Update last seen block number only if we processed blocks successfully
                        if let Some(&max_block) = blocks_to_process.iter().max() {
                            last_block_number = Some(U64::from(max_block));
                            debug!(
                                "✅ Updated last processed block to #{} for chain {}",
                                max_block, name
                            );
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
                        let delay = Duration::from_millis(50 * u64::pow(2, retry_count.min(4)));
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

    /// Process a batch of blocks with comprehensive protection (PARALLEL)
    async fn process_block_batch_with_protection(
        provider: &Provider<Http>,
        chain_name: &str,
        block_numbers: &[u64],
        topic_filters: &[H256],
        callback_tx: &mpsc::UnboundedSender<BlockProcessingData>,
        rate_limiter: &RateLimiter,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<usize, InjectorError> {
        // Process blocks in parallel with limited concurrency
        const MAX_CONCURRENT_BLOCKS: usize = 5;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_BLOCKS));
        
        let futures: Vec<_> = block_numbers
            .iter()
            .map(|&block_number| {
                let provider = provider.clone();
                let chain_name = chain_name.to_string();
                let topic_filters = topic_filters.to_vec();
                let callback_tx = callback_tx.clone();
                let rate_limiter = rate_limiter.clone();
                let circuit_breaker = circuit_breaker.clone();
                let semaphore = semaphore.clone();

                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    match Self::get_block_logs_with_protection(
                        &provider,
                        &chain_name,
                        block_number,
                        &topic_filters,
                        &rate_limiter,
                        &circuit_breaker,
                    )
                    .await
                    {
                        Ok((block, logs)) => {
                            if !logs.is_empty() {
                                let callback_data = BlockProcessingData {
                                    chain_name,
                                    block,
                                    logs,
                                    transactions: Vec::new(),
                                };

                                if callback_tx.send(callback_data).is_ok() {
                                    Some(1)
                                } else {
                                    warn!("Callback receiver dropped");
                                    None
                                }
                            } else {
                                Some(0)
                            }
                        }
                        Err(e) => {
                            warn!("Failed to process block #{}: {}", block_number, e);
                            None
                        }
                    }
                }
            })
            .collect();

        // Wait for all blocks to complete
        let results = join_all(futures).await;
        let processed_blocks = results.into_iter().flatten().sum();

        if processed_blocks > 0 {
            info!(
                "📦 Processed {} blocks with events for {} (PARALLEL)",
                processed_blocks,
                chain_name
            );
        }

        Ok(processed_blocks)
    }

    /// Get logs for a single block with protection
    async fn get_block_logs_with_protection(
        provider: &Provider<Http>,
        chain_name: &str,
        block_number: u64,
        topic_filters: &[H256],
        rate_limiter: &RateLimiter,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<(Block<H256>, Vec<Log>), InjectorError> {
        // Get block data
        let get_block = || async { provider.get_block(U64::from(block_number)).await };

        let block_result = Self::execute_rpc_with_protection(
            chain_name,
            rate_limiter,
            circuit_breaker,
            "get_block",
            get_block,
        )
        .await?;

        let block = block_result.ok_or_else(|| {
            InjectorError::NetworkError(format!("Block #{} not found", block_number))
        })?;

        // Get logs for the block
        let get_logs = || async {
            let filter = Filter::new()
                .from_block(U64::from(block_number))
                .to_block(U64::from(block_number));

            let filter_with_topics = if topic_filters.is_empty() {
                filter
            } else {
                filter.topic0(topic_filters.to_vec())
            };

            provider.get_logs(&filter_with_topics).await
        };

        let logs = Self::execute_rpc_with_protection(
            chain_name,
            rate_limiter,
            circuit_breaker,
            "get_logs",
            get_logs,
        )
        .await?;

        Ok((block, logs))
    }

    /// Fetch balances for multiple addresses from on-chain with proper rate limiting
    pub async fn fetch_balances_batch(
        &self,
        token: &str,
        addresses: &[&str],
    ) -> Result<HashMap<String, String>, InjectorError> {
        let provider = self.provider.clone().ok_or_else(|| {
            InjectorError::ConnectionError("Provider not initialized".to_string())
        })?;

        info!(
            "🔗 Fetching balances for {} unique members for token {}",
            addresses.len(),
            token
        );

        let token_address = Address::from_str(token).map_err(|e| {
            InjectorError::NetworkError(format!("Invalid token address {token}: {e}"))
        })?;

        let mut balances = HashMap::new();

        // Smaller batch size to avoid rate limiting - key change!
        const BATCH_SIZE: usize = 5; // Reduced from 50 to 5
        let max_concurrent = 3; // Limit concurrent requests
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        for (batch_idx, chunk) in addresses.chunks(BATCH_SIZE).enumerate() {
            debug!("🔄 Processing batch {}/{} with {} addresses", 
                   batch_idx + 1, 
                   (addresses.len() + BATCH_SIZE - 1) / BATCH_SIZE,
                   chunk.len());

            let mut batch_futures = Vec::new();

            for &address in chunk {
                if let Ok(addr) = Address::from_str(address) {
                    let provider = provider.clone();
                    let token_address = token_address;
                    let rate_limiter = self.rate_limiter.clone();
                    let semaphore = semaphore.clone();

                    let future = async move {
                        let _permit = semaphore.acquire().await.unwrap();
                        
                        // Use direct contract call with manual rate limiting
                        let _rate_guard = rate_limiter.acquire().await;
                        
                        let contract = ERC20Token::new(token_address, Arc::new(provider.as_ref().clone()));
                        match contract.balance_of(addr).call().await {
                            Ok(balance) => Some((address.to_string(), balance.to_string())),
                            Err(e) => {
                                // Check if it's a rate limit error
                                let error_msg = e.to_string();
                                if rate_limiter.is_rate_limit_error(&error_msg) {
                                    warn!("Rate limit hit during balance fetch for {}: {}", address, error_msg);
                                    rate_limiter.apply_backoff().await;
                                } else {
                                    warn!("Failed to fetch balance for {}: {}", address, e);
                                }
                                None
                            }
                        }
                    };

                    batch_futures.push(future);
                } else {
                    warn!("Invalid address format: {}", address);
                }
            }

            // Execute batch with proper concurrency control
            let batch_results = join_all(batch_futures).await;

            for result in batch_results.into_iter().flatten() {
                balances.insert(result.0, result.1);
            }

            // Adaptive delay between batches based on success rate
            let success_rate = balances.len() as f64 / ((batch_idx + 1) * BATCH_SIZE).min(addresses.len()) as f64;
            let delay_ms = if success_rate < 0.8 {
                500 // Longer delay if many failures
            } else {
                200 // Shorter delay if mostly successful
            };
            
            if batch_idx < addresses.chunks(BATCH_SIZE).len() - 1 {
                debug!("⏸️ Waiting {}ms before next batch (success rate: {:.1}%)", delay_ms, success_rate * 100.0);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        let success_rate = balances.len() as f64 / addresses.len() as f64;
        if success_rate >= 0.9 {
            info!("✅ Successfully fetched {} out of {} balances ({:.1}%)",
                  balances.len(), addresses.len(), success_rate * 100.0);
        } else {
            warn!("⚠️ Only fetched {} out of {} balances ({:.1}%) - consider adjusting rate limits",
                  balances.len(), addresses.len(), success_rate * 100.0);
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
    pub async fn eth_call(
        &self,
        to: &str,
        data: &str,
        block: Option<&str>,
    ) -> Result<String, InjectorError> {
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
                return Err(ethers::providers::ProviderError::CustomError(format!(
                    "RPC error: {}",
                    error
                )));
            }

            json.get("result")
                .and_then(|r| r.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    ethers::providers::ProviderError::CustomError(
                        "Invalid response format".to_string(),
                    )
                })
        };

        self.execute_rpc_call("eth_call", eth_call).await
    }

    /// Get ERC20 token balance using raw eth_call
    pub async fn get_token_balance(
        &self,
        token_address: &str,
        holder_address: &str,
    ) -> Result<String, InjectorError> {
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
                warn!(
                    "Failed to get token balance for {} on {}: {}",
                    holder_address, token_address, e
                );
                Ok("0".to_string()) // Return 0 on error rather than failing
            }
        }
    }

    /// Get voting power using raw eth_call for governance tokens
    pub async fn get_voting_power(
        &self,
        token_address: &str,
        holder_address: &str,
    ) -> Result<String, InjectorError> {
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
                warn!(
                    "Failed to get voting power for {} on {}: {}",
                    holder_address, token_address, e
                );
                Ok("0".to_string()) // Return 0 on error rather than failing
            }
        }
    }

    /// Get token balances for multiple addresses in batch (SIMPLIFIED)
    pub async fn get_balances_batch(
        &self,
        token_address: Address,
        member_addresses: &[Address],
        block_number: Option<u64>,
    ) -> Result<HashMap<Address, U256>, InjectorError> {
        if let Some(provider) = &self.provider {
            let provider_url = &provider.provider().url().to_string();

            match self
                .batch_helper
                .get_balances_batch(token_address, member_addresses, provider_url, block_number)
                .await
            {
                Ok(results) => Ok(results),
                Err(e) => {
                    error!("Batch balance request failed: {}", e);
                    Err(InjectorError::NetworkError(format!(
                        "Batch request failed: {}",
                        e
                    )))
                }
            }
        } else {
            Err(InjectorError::ConnectionError(
                "Provider not connected".to_string(),
            ))
        }
    }

    /// Get batch helper reference for custom batch operations
    pub fn batch_helper(&self) -> &Web3BatchHelper {
        &self.batch_helper
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
