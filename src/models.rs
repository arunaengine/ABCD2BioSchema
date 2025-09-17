use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
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


#[derive(Debug, Deserialize)]
pub struct Hook {
    pub hook_id: String,
    pub object: Resource,
    pub secret: String,
    pub download: Option<String>,
    pub pubkey_serial: i32,
    // TODO: scoped token
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}
