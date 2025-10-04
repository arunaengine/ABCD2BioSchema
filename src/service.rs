use std::sync::Arc;

use crate::Handler;
use crate::models::{ErrorResponse, Hook, JobResponse, TransformationParams, TransformationRequest};
use axum::Json;
use axum::extract::{Multipart, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use dotenvy::dotenv;
use tracing::{debug, info};

pub(crate) async fn upload_and_transform(
    State(state): State<Arc<Handler>>,
    Query(params): Query<TransformationParams>,
    multipart: Multipart,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    dotenv().ok();
    debug!("/transform endpoint called");
    debug!("\tReceived Params: {:?}", params);
    state.webhook.handle_transformation(multipart, params).await
}

pub(crate) async fn url_transform(
    State(state): State<Arc<Handler>>,
    Json(request): Json<Hook>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!("/transform/url endpoint called");
    debug!("\tReceived Request: {:?}", request);
    state.webhook.handle_url_transformation(request).await
}

pub(crate) async fn get_job_status(
    State(state): State<Arc<Handler>>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!("/job/{{job_id}} endpoint called");
    debug!("\tReceived Job ID: {}", job_id);
    state.webhook.get_job_status(&job_id).await
}

pub(crate) async fn health_check(State(_state): State<Arc<Handler>>) -> impl IntoResponse {
    let response = Json(serde_json::json!({
        "status": "ok",
        "message": "ABCD2BioSchema Service is running"
    }));
    debug!("/health endpoint called");
    debug!("\tResponse: {:?}", response);
    (StatusCode::OK, response)
}
