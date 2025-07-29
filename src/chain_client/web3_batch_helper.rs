use anyhow::Result;
use ethers::abi::{encode, Token, ParamType};
use ethers::types::{Address, U256};
use ethers::utils::keccak256;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::info;

#[derive(Debug, Clone)]
pub struct BatchRequestItem {
    pub method: String,
    pub params: Vec<Value>,
    pub identifier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponse<T> {
    pub identifier: String,
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<Value>,
}

#[derive(Clone)]
pub struct Web3BatchHelper {
    client: Client,
    default_batch_size: usize,
}

impl Web3BatchHelper {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            default_batch_size: 100,
        }
    }

    /// Execute a batch of RPC requests with adaptive batching
    pub async fn execute_batch<T>(
        &self,
        requests: Vec<BatchRequestItem>,
        provider_url: &str,
    ) -> Result<Vec<BatchResponse<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if requests.is_empty() {
            return Ok(vec![]);
        }

        self.execute_adaptive_batch(requests, provider_url, self.default_batch_size)
            .await
    }

    /// Execute batch with adaptive threshold reduction on errors
    async fn execute_adaptive_batch<T>(
        &self,
        requests: Vec<BatchRequestItem>,
        provider_url: &str,
        initial_threshold: usize,
    ) -> Result<Vec<BatchResponse<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut threshold = initial_threshold;
        let mut all_results = Vec::new();
        let mut remaining_requests = requests;
        let mut retry_count = 0;
        let max_retries = 5;

        while !remaining_requests.is_empty() && retry_count < max_retries {
            let batches = self.create_batches(&remaining_requests, threshold);
            let (failed_requests, batch_errors) =
                self.process_batches_sequentially(batches, provider_url, &mut all_results)
                    .await?;

            if failed_requests.is_empty() {
                break;
            }

            let should_reduce_batch = self.should_reduce_batch_size(&batch_errors, threshold);

            if should_reduce_batch && threshold > 1 {
                threshold = self.reduce_threshold(threshold);
                remaining_requests = failed_requests;
                retry_count += 1;
                info!("Reducing batch size to {} due to errors", threshold);
            } else {
                self.process_individual_requests(failed_requests, provider_url, &mut all_results)
                    .await?;
                remaining_requests = vec![];
            }
        }

        Ok(all_results)
    }

    /// Process failed requests individually as a final fallback
    async fn process_individual_requests<T>(
        &self,
        requests: Vec<BatchRequestItem>,
        provider_url: &str,
        all_results: &mut Vec<BatchResponse<T>>,
    ) -> Result<()>
    where
        T: for<'de> Deserialize<'de>,
    {
        for request in requests {
            match self.execute_single_batch(vec![request.clone()], provider_url).await {
                Ok(mut result) => all_results.append(&mut result),
                Err(e) => {
                    all_results.push(BatchResponse {
                        identifier: request.identifier,
                        success: false,
                        data: None,
                        error: Some(json!(e.to_string())),
                    });
                }
            }
        }
        Ok(())
    }

    /// Split requests into batches based on current threshold
    fn create_batches(
        &self,
        requests: &[BatchRequestItem],
        threshold: usize,
    ) -> Vec<Vec<BatchRequestItem>> {
        requests.chunks(threshold).map(|chunk| chunk.to_vec()).collect()
    }

    /// Process batches one by one and track results and errors
    async fn process_batches_sequentially<T>(
        &self,
        batches: Vec<Vec<BatchRequestItem>>,
        provider_url: &str,
        all_results: &mut Vec<BatchResponse<T>>,
    ) -> Result<(Vec<BatchRequestItem>, Vec<anyhow::Error>)>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut failed_requests = Vec::new();
        let mut batch_errors = Vec::new();

        for batch in batches {
            match self.execute_single_batch(batch.clone(), provider_url).await {
                Ok(mut batch_results) => all_results.append(&mut batch_results),
                Err(error) => {
                    failed_requests.extend(batch);
                    batch_errors.push(error);
                }
            }
        }

        Ok((failed_requests, batch_errors))
    }

    /// Determine if we should reduce batch size based on error types
    fn should_reduce_batch_size(&self, errors: &[anyhow::Error], current_threshold: usize) -> bool {
        if errors.is_empty() || current_threshold <= 1 {
            return false;
        }

        errors.iter().any(|error| {
            let message = error.to_string().to_lowercase();
            message.contains("timeout")
                || message.contains("response size")
                || message.contains("too large")
                || message.contains("exceeded")
                || message.contains("query returned more than")
        })
    }

    /// Reduce the batch threshold by half
    fn reduce_threshold(&self, current_threshold: usize) -> usize {
        std::cmp::max(current_threshold / 2, 1)
    }

    /// Execute a single batch (up to specified size)
    async fn execute_single_batch<T>(
        &self,
        requests: Vec<BatchRequestItem>,
        provider_url: &str,
    ) -> Result<Vec<BatchResponse<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let batch_requests: Vec<Value> = requests
            .iter()
            .map(|req| {
                json!({
                    "jsonrpc": "2.0",
                    "id": req.identifier.clone(),
                    "method": req.method,
                    "params": req.params
                })
            })
            .collect();

        let response = self
            .client
            .post(provider_url)
            .header("Content-Type", "application/json")
            .json(&batch_requests)
            .send()
            .await?;

        let response_data: Vec<Value> = response.json().await?;

        let results = requests
            .into_iter()
            .zip(response_data.into_iter())
            .map(|(req, rpc_result)| {
                if let Some(error) = rpc_result.get("error") {
                    BatchResponse {
                        identifier: req.identifier,
                        success: false,
                        data: None,
                        error: Some(error.clone()),
                    }
                } else if let Some(result) = rpc_result.get("result") {
                    match serde_json::from_value::<T>(result.clone()) {
                        Ok(data) => BatchResponse {
                            identifier: req.identifier,
                            success: true,
                            data: Some(data),
                            error: None,
                        },
                        Err(e) => BatchResponse {
                            identifier: req.identifier,
                            success: false,
                            data: None,
                            error: Some(json!(e.to_string())),
                        },
                    }
                } else {
                    BatchResponse {
                        identifier: req.identifier,
                        success: false,
                        data: None,
                        error: Some(json!("No result or error in response")),
                    }
                }
            })
            .collect();

        Ok(results)
    }

    /// Helper method for encoding function calls
    pub fn encode_function(
        &self,
        function_signature: &str,
        _param_types: &[&str],
        param_values: &[Token],
    ) -> Result<String> {
        let function_selector = format!("0x{:08x}", u32::from_be_bytes(
            keccak256(function_signature.as_bytes())[0..4].try_into().unwrap()
        ));
        
        let encoded_params = encode(param_values);
        let encoded_hex = hex::encode(encoded_params);
        
        Ok(format!("{}{}", function_selector, encoded_hex))
    }

    /// Helper method for decoding results
    pub fn decode_result(&self, types: &[&str], data: &str) -> Result<Vec<Token>> {
        let data_bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data))?;
        let param_types: Vec<ParamType> = types
            .iter()
            .map(|t| match *t {
                "uint256" => ParamType::Uint(256),
                "address" => ParamType::Address,
                "string" => ParamType::String,
                "bytes" => ParamType::Bytes,
                _ => ParamType::Uint(256), // Default fallback
            })
            .collect();
        let decoded = ethers::abi::decode(&param_types, &data_bytes)?;
        Ok(decoded)
    }

    /// Convenience method for batch eth_call requests
    pub async fn eth_call<T>(
        &self,
        calls: Vec<EthCallRequest>,
        provider_url: &str,
        block_tag: Option<&str>,
    ) -> Result<Vec<BatchResponse<T>>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let requests = calls
            .into_iter()
            .map(|call| BatchRequestItem {
                method: "eth_call".to_string(),
                params: vec![
                    json!({
                        "to": format!("0x{:040x}", call.to),
                        "data": call.data
                    }),
                    json!(block_tag.unwrap_or("latest")),
                ],
                identifier: call.identifier,
            })
            .collect();

        self.execute_batch(requests, provider_url).await
    }

    /// Get token balances for multiple addresses in batch (SIMPLIFIED)
    pub async fn get_balances_batch(
        &self,
        token_address: Address,
        member_addresses: &[Address],
        provider_url: &str,
        block_number: Option<u64>,
    ) -> Result<HashMap<Address, U256>> {
        if member_addresses.is_empty() {
            return Ok(HashMap::new());
        }

        // Create balance calls for all addresses
        let mut balance_calls = Vec::new();
        for (index, &member_address) in member_addresses.iter().enumerate() {
            let balance_data = self.encode_function(
                "balanceOf(address)",
                &["address"],
                &[Token::Address(member_address)],
            )?;
            
            balance_calls.push(EthCallRequest {
                to: token_address,
                data: balance_data,
                identifier: format!("balance_{}_{}", member_address, index),
            });
        }

        // Execute batch calls with the appropriate block tag
        let block_tag = match block_number {
            Some(num) => format!("0x{:x}", num),
            None => "latest".to_string(),
        };
        let balance_results = self.eth_call::<String>(balance_calls, provider_url, Some(&block_tag)).await?;

        // Process results
        let mut results = HashMap::new();
        for (index, &member_address) in member_addresses.iter().enumerate() {
            let balance_id = format!("balance_{}_{}", member_address, index);
            
            let balance = balance_results
                .iter()
                .find(|r| r.identifier == balance_id)
                .and_then(|r| r.data.as_ref())
                .and_then(|data| self.decode_result(&["uint256"], data).ok())
                .and_then(|tokens| tokens.get(0).cloned())
                .and_then(|token| match token {
                    Token::Uint(val) => Some(val),
                    _ => None,
                })
                .unwrap_or_else(|| U256::zero());

            results.insert(member_address, balance);
        }

        Ok(results)
    }
}

#[derive(Debug, Clone)]
pub struct EthCallRequest {
    pub to: Address,
    pub data: String,
    pub identifier: String,
}

impl Default for Web3BatchHelper {
    fn default() -> Self {
        Self::new()
    }
}