use crate::database::{DatabaseClient, ChainClientRegistry};
use crate::models::*;
use crate::source::MongoClient;
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

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseClient,
    pub source_db: Option<MongoClient>,
    pub chain_clients: ChainClientRegistry,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/sync/historical", post(sync_historical))
        .route("/status/{job_id}", get(get_job_status)) // Changed from :job_id to {job_id}
        .route("/health", get(health_check))
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
    if (state.db.sync_jobs().insert_one(&job).await).is_err() {
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
        .db
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

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
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
        .source_db
        .as_ref()
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
        .source_db
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let results = source_db
        .aggregate(&request.model, request.pipeline)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(results))
}
