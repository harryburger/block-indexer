use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};
use rand::Rng;

#[derive(Debug, Clone)]
pub enum CrawlerError {
    BatchSizeError {
        message: String,
        suggested_chunk_size: Option<u64>,
    },
    RateLimitError {
        message: String,
        retry_after_ms: Option<u64>,
    },
    NetworkError {
        message: String,
        is_retryable: bool,
    },
    ParseError {
        message: String,
    },
    ConfigurationError {
        message: String,
    },
}

impl std::fmt::Display for CrawlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrawlerError::BatchSizeError { message, .. } => write!(f, "Batch size error: {}", message),
            CrawlerError::RateLimitError { message, .. } => write!(f, "Rate limit error: {}", message),
            CrawlerError::NetworkError { message, .. } => write!(f, "Network error: {}", message),
            CrawlerError::ParseError { message } => write!(f, "Parse error: {}", message),
            CrawlerError::ConfigurationError { message } => write!(f, "Configuration error: {}", message),
        }
    }
}

impl std::error::Error for CrawlerError {}

impl CrawlerError {
    pub fn classify_rpc_error(error_message: &str) -> Self {
        let error_lower = error_message.to_lowercase();
        
        // Rate limiting patterns
        if error_lower.contains("compute units per second")
            || error_lower.contains("call rate limit exhausted")
            || error_lower.contains("rate limiting")
            || error_lower.contains("throttled")
            || error_lower.contains("too many requests")
        {
            return CrawlerError::RateLimitError {
                message: error_message.to_string(),
                retry_after_ms: None,
            };
        }
        
        // Batch size error patterns
        if error_lower.contains("query returned more than")
            || error_lower.contains("response size is larger than")
            || error_lower.contains("query timed out")
            || error_lower.contains("eth_getlogs is limited")
            || error_lower.contains("consider reducing your block range")
            || error_lower.contains("results limit")
        {
            return CrawlerError::BatchSizeError {
                message: error_message.to_string(),
                suggested_chunk_size: None,
            };
        }
        
        // Network errors (mostly retryable)
        if error_lower.contains("connection")
            || error_lower.contains("timeout")
            || error_lower.contains("network")
            || error_lower.contains("dns")
        {
            return CrawlerError::NetworkError {
                message: error_message.to_string(),
                is_retryable: true,
            };
        }
        
        // Default to parse error
        CrawlerError::ParseError {
            message: error_message.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    #[serde(rename = "blockNumber")]
    pub block_number: String,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: String,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "logIndex")]
    pub log_index: String,
    pub removed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFilter {
    #[serde(rename = "fromBlock")]
    pub from_block: String,
    #[serde(rename = "toBlock")]
    pub to_block: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<Option<String>>>,
}

#[derive(Debug, Clone)]
pub struct CrawlerStats {
    pub total_blocks_processed: u64,
    pub total_logs_found: u64,
    pub total_requests_made: u32,
    pub successful_requests: u32,
    pub failed_requests: u32,
    pub chunk_size_reductions: u32,
    pub rate_limit_hits: u32,
    pub batch_size_errors: u32,
    pub average_logs_per_second: f64,
    pub total_duration: Duration,
}

impl Default for CrawlerStats {
    fn default() -> Self {
        Self {
            total_blocks_processed: 0,
            total_logs_found: 0,
            total_requests_made: 0,
            successful_requests: 0,
            failed_requests: 0,
            chunk_size_reductions: 0,
            rate_limit_hits: 0,
            batch_size_errors: 0,
            average_logs_per_second: 0.0,
            total_duration: Duration::new(0, 0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveCrawler {
    rpc_url: String,
    range: BlockRange,
    filters: LogFilters,
    strategy: AdaptiveStrategy,
    client: Client,
    consecutive_successes: u32,
}

#[derive(Debug, Clone)]
pub struct AdaptiveStrategy {
    pub chunk_size: ChunkSizeConfig,
    pub retry_config: RetryConfig,
    pub backoff_config: BackoffConfig,
    pub concurrency: ConcurrencyConfig,
}

#[derive(Debug, Clone)]
pub struct BlockRange {
    pub from_block: u64,
    pub to_block: u64,
}

#[derive(Debug, Clone)]
pub struct LogFilters {
    pub topics: Vec<String>,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSizeConfig {
    pub current: u64,
    pub initial: u64,
    pub min: u64,
    pub max: u64,
    pub reduction_factor: f64,
    pub recovery_threshold: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    pub max_delay_ms: u64,
    pub multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    pub max_concurrent_requests: usize,
    pub max_concurrent: usize,
    pub rate_limit_per_second: f64,
}

impl Default for ChunkSizeConfig {
    fn default() -> Self {
        Self {
            current: 1000,
            initial: 1000,
            min: 10,
            max: 1000,
            reduction_factor: 0.5,
            recovery_threshold: 3,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_delay_ms: 30000,
            multiplier: 2.0,
        }
    }
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 10,
            max_concurrent: 10,
            rate_limit_per_second: 100.0,
        }
    }
}

impl Default for AdaptiveStrategy {
    fn default() -> Self {
        Self {
            chunk_size: ChunkSizeConfig::default(),
            retry_config: RetryConfig::default(),
            backoff_config: BackoffConfig::default(),
            concurrency: ConcurrencyConfig::default(),
        }
    }
}

impl AdaptiveStrategy {
    pub fn handle_batch_size_error(&mut self) {
        let new_size = (self.chunk_size.current as f64 * self.chunk_size.reduction_factor) as u64;
        self.chunk_size.current = std::cmp::max(self.chunk_size.min, new_size);
        warn!(
            "Reducing chunk size due to batch error: {} -> {}",
            (self.chunk_size.current as f64 / self.chunk_size.reduction_factor) as u64,
            self.chunk_size.current
        );
    }
    
    pub fn handle_success(&mut self, consecutive_successes: &mut u32) {
        *consecutive_successes += 1;
        
        if *consecutive_successes >= self.chunk_size.recovery_threshold 
            && self.chunk_size.current < self.chunk_size.max 
        {
            let old_size = self.chunk_size.current;
            self.chunk_size.current = std::cmp::min(
                self.chunk_size.max,
                self.chunk_size.current * 2
            );
            *consecutive_successes = 0;
            
            if old_size != self.chunk_size.current {
                info!(
                    "Increasing chunk size after {} successes: {} -> {}",
                    self.chunk_size.recovery_threshold,
                    old_size,
                    self.chunk_size.current
                );
            }
        }
    }
    
    pub fn reset_consecutive_successes(&self, consecutive_successes: &mut u32) {
        *consecutive_successes = 0;
    }
    
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.retry_config.max_retries
    }
    
    pub fn calculate_backoff_delay(&self, attempt: u32) -> Duration {
        let base_delay = self.retry_config.initial_delay_ms as f64;
        let multiplier = self.retry_config.backoff_multiplier;
        let max_delay = self.retry_config.max_delay_ms;
        
        let delay_ms = (base_delay * multiplier.powi(attempt as i32)) as u64;
        let capped_delay = std::cmp::min(delay_ms, max_delay);
        
        // Add jitter to prevent thundering herd
        let mut rng = rand::rng();
        let jitter = (capped_delay as f64 * 0.1 * rng.random::<f64>()) as u64;
        Duration::from_millis(capped_delay + jitter)
    }
}

impl AdaptiveCrawler {
    pub fn new(
        rpc_url: String,
        range: BlockRange,
        filters: LogFilters,
        strategy: AdaptiveStrategy,
    ) -> Result<Self, CrawlerError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| CrawlerError::ConfigurationError {
                message: format!("Failed to create HTTP client: {}", e),
            })?;
            
        Ok(Self {
            rpc_url,
            range,
            filters,
            strategy,
            client,
            consecutive_successes: 0,
        })
    }
    
    fn create_log_filter(&self, from_block: u64, to_block: u64) -> LogFilter {
        LogFilter {
            from_block: format!("0x{:x}", from_block),
            to_block: format!("0x{:x}", to_block),
            address: if self.filters.addresses.is_empty() {
                None
            } else {
                Some(self.filters.addresses.clone())
            },
            topics: if self.filters.topics.is_empty() {
                None
            } else {
                Some(self.filters.topics.iter().map(|t| Some(t.clone())).collect())
            },
        }
    }
    
    fn create_batch_requests(&self, from: u64, to: u64) -> Vec<JsonRpcRequest> {
        let chunk_size = self.strategy.chunk_size.current;
        let mut requests = Vec::new();
        let mut current_block = from;
        
        while current_block <= to {
            let chunk_end = std::cmp::min(current_block + chunk_size - 1, to);
            
            let filter = self.create_log_filter(current_block, chunk_end);
            let request = JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                id: format!("logs_{}_{}", current_block, chunk_end),
                method: "eth_getLogs".to_string(),
                params: vec![serde_json::to_value(&filter).unwrap()],
            };
            
            requests.push(request);
            current_block = chunk_end + 1;
        }
        
        requests
    }
    
    async fn send_batch_request(
        &self,
        requests: Vec<JsonRpcRequest>,
    ) -> Result<Vec<JsonRpcResponse>, CrawlerError> {
        let response = self
            .client
            .post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&requests)
            .send()
            .await
            .map_err(|e| CrawlerError::NetworkError {
                message: format!("HTTP request failed: {}", e),
                is_retryable: true,
            })?;
            
        if !response.status().is_success() {
            return Err(CrawlerError::NetworkError {
                message: format!("HTTP {} error: {}", response.status(), response.status()),
                is_retryable: response.status().is_server_error(),
            });
        }
        
        let responses: Vec<JsonRpcResponse> = response
            .json()
            .await
            .map_err(|e| CrawlerError::ParseError {
                message: format!("Failed to parse JSON response: {}", e),
            })?;
            
        Ok(responses)
    }
    
    fn process_batch_responses(
        &self,
        responses: Vec<JsonRpcResponse>,
    ) -> Result<Vec<EthereumLog>, CrawlerError> {
        let mut all_logs = Vec::new();
        
        for response in responses {
            if let Some(error) = response.error {
                return Err(CrawlerError::classify_rpc_error(&error.message));
            }
            
            if let Some(result) = response.result {
                let logs: Vec<EthereumLog> = serde_json::from_value(result)
                    .map_err(|e| CrawlerError::ParseError {
                        message: format!("Failed to parse logs from response: {}", e),
                    })?;
                all_logs.extend(logs);
            }
        }
        
        Ok(all_logs)
    }
    
    async fn crawl_chunk_with_retry(
        &mut self,
        from: u64,
        to: u64,
        stats: &mut CrawlerStats,
    ) -> Result<Vec<EthereumLog>, CrawlerError> {
        let mut attempt = 0;
        
        loop {
            stats.total_requests_made += 1;
            
            let requests = self.create_batch_requests(from, to);
            
            match self.send_batch_request(requests).await {
                Ok(responses) => {
                    match self.process_batch_responses(responses) {
                        Ok(logs) => {
                            stats.successful_requests += 1;
                            self.strategy.handle_success(&mut self.consecutive_successes);
                            return Ok(logs);
                        }
                        Err(CrawlerError::BatchSizeError { .. }) => {
                            stats.failed_requests += 1;
                            stats.batch_size_errors += 1;
                            
                            // Handle batch size error immediately
                            self.strategy.handle_batch_size_error();
                            stats.chunk_size_reductions += 1;
                            self.strategy.reset_consecutive_successes(&mut self.consecutive_successes);
                            
                            // If chunk size is at minimum, this is a hard error
                            if self.strategy.chunk_size.current <= self.strategy.chunk_size.min {
                                return Err(CrawlerError::BatchSizeError {
                                    message: "Chunk size at minimum, cannot reduce further".to_string(),
                                    suggested_chunk_size: Some(self.strategy.chunk_size.min),
                                });
                            }
                            
                            // Retry immediately with reduced chunk size
                            continue;
                        }
                        Err(CrawlerError::RateLimitError { message, retry_after_ms }) => {
                            stats.failed_requests += 1;
                            stats.rate_limit_hits += 1;
                            
                            if !self.strategy.should_retry(attempt) {
                                return Err(CrawlerError::RateLimitError { message, retry_after_ms });
                            }
                            
                            let delay = if let Some(retry_after) = retry_after_ms {
                                Duration::from_millis(retry_after)
                            } else {
                                self.strategy.calculate_backoff_delay(attempt)
                            };
                            
                            warn!("Rate limited, waiting {:?} before retry {}", delay, attempt + 1);
                            sleep(delay).await;
                            attempt += 1;
                        }
                        Err(CrawlerError::NetworkError { message, is_retryable }) => {
                            stats.failed_requests += 1;
                            
                            if !is_retryable || !self.strategy.should_retry(attempt) {
                                return Err(CrawlerError::NetworkError { message, is_retryable });
                            }
                            
                            let delay = self.strategy.calculate_backoff_delay(attempt);
                            warn!("Network error, waiting {:?} before retry {}: {}", delay, attempt + 1, message);
                            sleep(delay).await;
                            attempt += 1;
                        }
                        Err(e) => {
                            stats.failed_requests += 1;
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    stats.failed_requests += 1;
                    
                    match &e {
                        CrawlerError::NetworkError { is_retryable, .. } if *is_retryable => {
                            if !self.strategy.should_retry(attempt) {
                                return Err(e);
                            }
                            
                            let delay = self.strategy.calculate_backoff_delay(attempt);
                            warn!("Request failed, waiting {:?} before retry {}: {}", delay, attempt + 1, e);
                            sleep(delay).await;
                            attempt += 1;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }
    }
    
    pub async fn crawl_logs(&mut self) -> Result<Vec<EthereumLog>, CrawlerError> {
        let start_time = Instant::now();
        let mut stats = CrawlerStats::default();
        let mut all_logs = Vec::new();
        
        let total_blocks = self.range.to_block - self.range.from_block + 1;
        info!(
            "Starting adaptive crawl: blocks {} to {} ({} total blocks)",
            self.range.from_block, self.range.to_block, total_blocks
        );
        
        let mut current_block = self.range.from_block;
        
        while current_block <= self.range.to_block {
            let chunk_end = std::cmp::min(
                current_block + self.strategy.chunk_size.current - 1,
                self.range.to_block,
            );
            
            info!(
                "Crawling chunk: {} to {} (chunk size: {})",
                current_block, chunk_end, self.strategy.chunk_size.current
            );
            
            match self.crawl_chunk_with_retry(current_block, chunk_end, &mut stats).await {
                Ok(logs) => {
                    let logs_count = logs.len();
                    all_logs.extend(logs);
                    
                    let blocks_processed = chunk_end - current_block + 1;
                    stats.total_blocks_processed += blocks_processed;
                    stats.total_logs_found += logs_count as u64;
                    
                    info!(
                        "Chunk complete: {} logs found, {} blocks processed",
                        logs_count, blocks_processed
                    );
                    
                    current_block = chunk_end + 1;
                }
                Err(CrawlerError::BatchSizeError { message, .. }) => {
                    error!("Batch size error at minimum chunk size: {}", message);
                    return Err(CrawlerError::BatchSizeError {
                        message: format!(
                            "Cannot process blocks {} to {}: {}",
                            current_block, chunk_end, message
                        ),
                        suggested_chunk_size: Some(self.strategy.chunk_size.min),
                    });
                }
                Err(e) => {
                    error!("Crawling failed at blocks {} to {}: {}", current_block, chunk_end, e);
                    return Err(e);
                }
            }
        }
        
        stats.total_duration = start_time.elapsed();
        let logs_per_second = if stats.total_duration.as_secs() > 0 {
            stats.total_logs_found as f64 / stats.total_duration.as_secs() as f64
        } else {
            0.0
        };
        stats.average_logs_per_second = logs_per_second;
        
        info!(
            "Crawl complete: {} logs found, {} blocks processed in {:?} ({:.2} logs/sec)",
            stats.total_logs_found,
            stats.total_blocks_processed,
            stats.total_duration,
            logs_per_second
        );
        
        self.print_stats(&stats);
        
        Ok(all_logs)
    }
    
    pub fn get_stats(&self) -> CrawlerStats {
        CrawlerStats::default()
    }
    
    fn print_stats(&self, stats: &CrawlerStats) {
        info!("=== Crawler Statistics ===");
        info!("Total requests: {}", stats.total_requests_made);
        info!("Successful requests: {}", stats.successful_requests);
        info!("Failed requests: {}", stats.failed_requests);
        info!("Success rate: {:.2}%", 
            if stats.total_requests_made > 0 {
                (stats.successful_requests as f64 / stats.total_requests_made as f64) * 100.0
            } else {
                0.0
            }
        );
        info!("Batch size errors: {}", stats.batch_size_errors);
        info!("Rate limit hits: {}", stats.rate_limit_hits);
        info!("Chunk size reductions: {}", stats.chunk_size_reductions);
        info!("Final chunk size: {}", self.strategy.chunk_size.current);
        info!("Average logs per second: {:.2}", stats.average_logs_per_second);
        info!("========================");
    }
}

pub struct CrawlerBuilder {
    rpc_url: Option<String>,
    range: Option<BlockRange>,
    filters: LogFilters,
    strategy: AdaptiveStrategy,
}

impl CrawlerBuilder {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url: Some(rpc_url),
            range: None,
            filters: LogFilters {
                topics: Vec::new(),
                addresses: Vec::new(),
            },
            strategy: AdaptiveStrategy::default(),
        }
    }
    
    pub fn range(mut self, from: u64, to: u64) -> Self {
        self.range = Some(BlockRange {
            from_block: from,
            to_block: to,
        });
        self
    }
    
    pub fn topics(mut self, topics: Vec<String>) -> Self {
        self.filters.topics = topics;
        self
    }
    
    pub fn addresses(mut self, addresses: Vec<String>) -> Self {
        self.filters.addresses = addresses;
        self
    }
    
    pub fn chunk_size(mut self, initial: u64) -> Self {
        self.strategy.chunk_size.current = initial;
        self
    }
    
    pub fn chunk_size_limits(mut self, min: u64, max: u64) -> Self {
        self.strategy.chunk_size.min = min;
        self.strategy.chunk_size.max = max;
        self
    }
    
    pub fn chunk_size_reduction_factor(mut self, factor: f64) -> Self {
        self.strategy.chunk_size.reduction_factor = factor;
        self
    }
    
    pub fn retry_config(mut self, max_retries: u32, initial_delay_ms: u64) -> Self {
        self.strategy.retry_config.max_retries = max_retries;
        self.strategy.retry_config.initial_delay_ms = initial_delay_ms;
        self
    }
    
    pub fn backoff_config(mut self, max_delay_ms: u64, multiplier: f64) -> Self {
        self.strategy.backoff_config.max_delay_ms = max_delay_ms;
        self.strategy.backoff_config.multiplier = multiplier;
        self.strategy.retry_config.max_delay_ms = max_delay_ms;
        self.strategy.retry_config.backoff_multiplier = multiplier;
        self
    }
    
    pub fn max_concurrent_requests(mut self, max: usize) -> Self {
        self.strategy.concurrency.max_concurrent_requests = max;
        self
    }
    
    pub fn recovery_threshold(mut self, threshold: u32) -> Self {
        self.strategy.chunk_size.recovery_threshold = threshold;
        self
    }
    
    pub fn build(self) -> Result<AdaptiveCrawler, CrawlerError> {
        let rpc_url = self.rpc_url.ok_or_else(|| CrawlerError::ConfigurationError {
            message: "RPC URL is required".to_string(),
        })?;
        
        let range = self.range.ok_or_else(|| CrawlerError::ConfigurationError {
            message: "Block range is required".to_string(),
        })?;
        
        if range.from_block > range.to_block {
            return Err(CrawlerError::ConfigurationError {
                message: "Invalid range: from_block must be <= to_block".to_string(),
            });
        }
        
        if self.strategy.chunk_size.min > self.strategy.chunk_size.max {
            return Err(CrawlerError::ConfigurationError {
                message: "Invalid chunk size limits: min must be <= max".to_string(),
            });
        }
        
        if self.strategy.chunk_size.current < self.strategy.chunk_size.min 
            || self.strategy.chunk_size.current > self.strategy.chunk_size.max {
            return Err(CrawlerError::ConfigurationError {
                message: "Initial chunk size must be within min/max limits".to_string(),
            });
        }
        
        AdaptiveCrawler::new(rpc_url, range, self.filters, self.strategy)
    }
}