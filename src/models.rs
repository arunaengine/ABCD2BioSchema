use serde::{Deserialize, Serialize};
use crate::job::Job;

#[derive(Debug, Deserialize)]
pub struct TransformationParams {
    pub transformation_id: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransformationRequest {
    pub xml_url: String,
    pub transformation_id: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobResponse {
    pub job: Job,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}