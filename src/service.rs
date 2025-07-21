use axum::extract::{Multipart, Query};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use dotenvy::dotenv;
use crate::models::{ErrorResponse, JobResponse, TransformationParams, TransformationRequest};
use crate::webhook::GfbioWebhook;

pub(crate) async fn upload_and_transform(
    Query(params): Query<TransformationParams>,
    multipart: Multipart,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    dotenv().ok();
    let api_base_url = dotenvy::var("GFBIO_BASE_URL").expect("GFBIO_BASE_URL must be set");
    let temp_dir = dotenvy::var("TEMP_DIR").expect("TEMP_DIR must be set");
    let webhook = GfbioWebhook::with_config(api_base_url, temp_dir);
    webhook.handle_transformation(multipart, params).await
}

pub(crate) async fn url_transform(
    Json(request): Json<TransformationRequest>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    let api_base_url = dotenvy::var("GFBIO_BASE_URL").expect("GFBIO_BASE_URL must be set");
    let temp_dir = dotenvy::var("TEMP_DIR").expect("TEMP_DIR must be set");
    let webhook = GfbioWebhook::with_config(api_base_url, temp_dir);
    webhook.handle_url_transformation(request).await
}

pub(crate) async fn get_job_status(
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    let api_base_url = dotenvy::var("GFBIO_BASE_URL").expect("GFBIO_BASE_URL must be set");
    let temp_dir = dotenvy::var("TEMP_DIR").expect("TEMP_DIR must be set");
    let webhook = GfbioWebhook::with_config(api_base_url, temp_dir);
    webhook.get_job_status(&job_id).await
}

pub(crate) async fn health_check() -> impl IntoResponse {
    let response = Json(serde_json::json!({
        "status": "ok",
        "message": "ABCD2BioSchema Service is running"
    }));
    (StatusCode::OK, response)
}