use std::sync::Arc;

use crate::Handler;
use crate::models::{ErrorResponse, Hook, JobResponse, TransformationRequest};
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use dotenvy::dotenv;
use tokio::sync::Mutex;

pub(crate) async fn upload_and_transform(
    State(state): State<Arc<Mutex<Handler>>>,
    Json(request): Json<Hook>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    dotenv().ok();
    state.lock().await.webhook.handle_transformation(request).await
}

pub(crate) async fn url_transform(
    State(state): State<Arc<Mutex<Handler>>>,
    Json(request): Json<TransformationRequest>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    state.lock().await.webhook.handle_url_transformation(request).await
}

pub(crate) async fn get_job_status(
    State(state): State<Arc<Mutex<Handler>>>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    state.lock().await.webhook.get_job_status(&job_id).await
}

pub(crate) async fn health_check(State(state): State<Arc<Mutex<Handler>>>) -> impl IntoResponse {
    let response = Json(serde_json::json!({
        "status": "ok",
        "message": "ABCD2BioSchema Service is running"
    }));
    (StatusCode::OK, response)
}
