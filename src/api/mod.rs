use crate::app_state::AppState;
use crate::models::*;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use bson::DateTime as BsonDateTime;
use serde_json::Value;
use uuid::Uuid;


pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/sync/historical", post(sync_historical))
        .route("/status/{job_id}", get(get_job_status)) // Changed from :job_id to {job_id}
        .route("/health", get(health_check))
        .route("/metrics/rate-limiting", get(get_rate_limiting_metrics))
        .route("/source/query", post(source_query))
        .route("/source/aggregate", post(source_aggregate))
        .with_state(state)
}

async fn sync_historical(
    State(state): State<AppState>,
    Json(request): Json<HistoricalSyncRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let job_id = Uuid::new_v4();
    let current_time = BsonDateTime::now();

    let job = SyncJob {
        id: None,                   // MongoDB will generate this
        job_id: job_id.to_string(), // UUID as string
        network: request.network,
        token_address: request.token_address,
        from_block: request.from_block,
        to_block: request.to_block,
        status: JobStatus::Pending,
        created_at: current_time,
        updated_at: current_time,
        error: None,
    };

    // Store job in database only (no queue)
    if (state.db().sync_jobs().insert_one(&job).await).is_err() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Note: Job processing would need to be handled differently
    // For now, just mark as completed immediately or implement a different processing mechanism

    Ok(Json(serde_json::json!({
        "jobId": job_id,
        "status": "stored"
    })))
}

async fn get_job_status(
    Path(job_id): Path<String>, // Changed from Uuid to String for simplicity
    State(state): State<AppState>,
) -> Result<Json<JobStatusResponse>, StatusCode> {
    use bson::doc;

    let job = state
        .db()
        .sync_jobs()
        .find_one(doc! { "job_id": job_id })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(job) = job {
        let progress = match job.status {
            JobStatus::Pending => Some(0.0),
            JobStatus::Processing => Some(0.5),
            JobStatus::Completed => Some(1.0),
            JobStatus::Failed => None,
        };

        Ok(Json(JobStatusResponse {
            id: job.job_id,
            status: job.status,
            progress,
            error: job.error,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut health_data = serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    // Add database health check
    if let Ok(db_healthy) = state.db().health_check().await {
        health_data["database"] = serde_json::json!({
            "status": if db_healthy { "connected" } else { "disconnected" }
        });

        // Add connection pool stats if available
        if let Ok(pool_stats) = state.db().get_pool_stats().await {
            health_data["database"]["pool_stats"] = serde_json::Value::String(pool_stats);
        }
    }

    // Add source database health if available
    if let Some(source_db) = state.source_db() {
        let source_healthy = source_db.database.run_command(bson::doc! { "ping": 1 }).await.is_ok();
        health_data["source_database"] = serde_json::json!({
            "status": if source_healthy { "connected" } else { "disconnected" }
        });
    }

    Json(health_data)
}

#[derive(serde::Deserialize)]
pub struct SourceQueryRequest {
    pub model: String,
    pub query: Value,
}

#[derive(serde::Deserialize)]
pub struct SourceAggregateRequest {
    pub model: String,
    pub pipeline: Vec<Value>,
}

async fn source_query(
    State(state): State<AppState>,
    Json(request): Json<SourceQueryRequest>,
) -> Result<Json<Vec<bson::Document>>, StatusCode> {
    let source_db = state
        .source_db()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let results = source_db
        .query(&request.model, request.query)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(results))
}

async fn source_aggregate(
    State(state): State<AppState>,
    Json(request): Json<SourceAggregateRequest>,
) -> Result<Json<Vec<bson::Document>>, StatusCode> {
    let source_db = state
        .source_db()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let results = source_db
        .aggregate(&request.model, request.pipeline)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(results))
}

async fn get_rate_limiting_metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut chain_metrics = serde_json::Map::new();
    
    // Get metrics from each chain client
    for (chain_name, client_arc) in state.chain_clients().iter() {
        if let Ok(_client) = client_arc.try_lock() {
            // We can't directly access rate_limiter from the client since it's private
            // Instead, we'll add a method to get metrics in the future
            let metrics_placeholder = serde_json::json!({
                "status": "metrics collection in progress",
                "chain": chain_name
            });
            chain_metrics.insert(chain_name.clone(), metrics_placeholder);
        } else {
            chain_metrics.insert(chain_name.clone(), serde_json::json!({
                "status": "client locked",
                "chain": chain_name
            }));
        }
    }

    Json(serde_json::json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "chains": chain_metrics
    }))
}
