use crate::job::Job;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use serde::{Deserialize, Serialize};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};

#[derive(Debug, Deserialize)]
pub struct TransformationParams {
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
    pub token: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Clone)]
pub struct ClientInterceptor {
    pub api_token: String,
}
// Implement a request interceptor which always adds
//  the authorization header with a specific API token to all requests
impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
            AsciiMetadataValue::try_from(format!("Bearer {}", self.api_token.as_str())).unwrap(),
        );

        Ok(mut_req)
    }
}
