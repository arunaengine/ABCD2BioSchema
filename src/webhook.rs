use crate::job::Job;
use crate::models::{
    ErrorResponse, Hook, JobResponse, TransformationParams, TransformationRequest,
};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use axum::Json;
use axum::extract::Multipart;
use axum::http::StatusCode;
use chrono::FixedOffset;
use reqwest::Client;
use tokio::fs;
use uuid::Uuid;

pub struct GfbioWebhook {
    client: Client,
    gfbio_base_url: String,
    temp_dir: String,
}

impl GfbioWebhook {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url: "https://transformation.gfbio.org/api".to_string(),
            temp_dir: "./temp".to_string(),
        }
    }

    pub fn with_config(gfbio_base_url: String, temp_dir: String) -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url,
            temp_dir,
        }
    }

    async fn save_temp_file(
        &self,
        filename: &str,
        content: &[u8],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        fs::create_dir_all(&self.temp_dir).await?;

        let file_id = Uuid::new_v4().to_string();
        let temp_filename = format!("{}_{}", file_id, filename);
        let temp_path = format!("{}/{}", self.temp_dir, temp_filename);

        fs::write(&temp_path, content).await?;

        Ok(format!("file://{}", temp_path))
    }

    async fn download_xml(
        &self,
        url: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .client
            .get(url)
            .header("User-Agent", "GFBio-Webhook/1.0")
            .send()
            .await?;

        if response.status().is_success() {
            let content = response.bytes().await?;
            Ok(content.to_vec())
        } else {
            Err(format!("Failed to download XML: {}", response.status()).into())
        }
    }

    async fn send_gfbio_request(
        &self,
        input_file_url: &str,
        transformation_id: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let query_url = format!(
            "{}/transform?transformation={}&input_file_url={}&input_file_zipped=false",
            self.gfbio_base_url, transformation_id, input_file_url
        );

        println!("Sending request to GFBio API: {}", query_url);

        let response = self
            .client
            .get(&query_url)
            .header("User-Agent", "GFBio-Webhook/1.0")
            .send()
            .await?;

        if response.status().is_success() {
            let json_response: serde_json::Value = response.json().await?;
            Ok(json_response)
        } else {
            Err(format!("GFBio API error: {}", response.status()).into())
        }
    }

    async fn fetch_result_data(
        &self,
        job_id: &str,
        result_file: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/results/{}/{}", self.gfbio_base_url, job_id, result_file);

        println!("Fetching result from: {}", url);

        let response = self
            .client
            .get(&url)
            .header("User-Agent", "GFBio-Webhook/1.0")
            .send()
            .await?;

        if response.status().is_success() {
            let json_response: serde_json::Value = response.json().await?;
            println!("Result data fetched successfully:\n{:#?}", json_response);
            Ok(json_response)
        } else {
            Err(format!("Failed to fetch result data: {}", response.status()).into())
        }
    }

    fn create_job_from_response(
        &self,
        gfbio_response: Option<serde_json::Value>,
        input_file_url: &str,
        transformation_id: &str,
        filename: &str,
    ) -> Job {
        let now = chrono::Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap());

        let (job_id, status, result_file, finish_time) = if let Some(response) = gfbio_response {
            if let Some(job_data) = response.get("job") {
                let job_id = job_data
                    .get("job_id")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                let status = job_data
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("pending")
                    .to_string();

                let result_file = job_data
                    .get("result_file")
                    .and_then(|s| s.as_str())
                    .unwrap_or("output/result.json")
                    .to_string();

                let finish_time = if status == "complete" {
                    Some(now + chrono::Duration::seconds(2))
                } else {
                    None
                };

                (job_id, status, result_file, finish_time)
            } else {
                (
                    "unknown".to_string(),
                    "processing".to_string(),
                    "output/result.json".to_string(),
                    None,
                )
            }
        } else {
            (
                "unknown".to_string(),
                "processing".to_string(),
                "output/result.json".to_string(),
                None,
            )
        };

        let query = format!(
            "{}/transform?transformation={}&input_file_url={}&input_file_zipped=false",
            self.gfbio_base_url, transformation_id, input_file_url
        );

        Job {
            job_id: job_id.clone(),
            transformation_id: transformation_id.to_string(),
            version_id: "1".to_string(),
            input_file_url: input_file_url.to_string(),
            input_file_zipped: "false".to_string(),
            query,
            input_file: format!("input/{}", filename.split('.').next().unwrap_or("unknown")),
            status,
            start_time: now,
            result_file,
            finish_time,
            combined_download: format!("{}.zip", job_id),
            job_expiration_date: now + chrono::Duration::days(1),
        }
    }

    fn generate_job_id(&self) -> String {
        let timestamp = chrono::Utc::now().timestamp();
        format!("{}", timestamp.abs())
    }

    pub async fn handle_url_transformation(
        &self,
        request: TransformationRequest,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        let transformation_id = request.transformation_id.unwrap_or_else(|| "5".to_string());

        if !request.xml_url.starts_with("http") {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_url".to_string(),
                    message: "URL must start with http or https".to_string(),
                }),
            ));
        }

        let xml_content = self.download_xml(&request.xml_url).await.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "download_error".to_string(),
                    message: format!("Failed to download XML: {}", e),
                }),
            )
        })?;

        if xml_content.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "empty_file".to_string(),
                    message: "Downloaded XML is empty".to_string(),
                }),
            ));
        }

        let filename = request
            .xml_url
            .split('/')
            .last()
            .unwrap_or("downloaded.xml")
            .to_string();

        let gfbio_response = match self
            .send_gfbio_request(&request.xml_url, &transformation_id)
            .await
        {
            Ok(response) => Some(response),
            Err(e) => {
                eprintln!("GFBio API error: {}", e);
                None
            }
        };

        let job = self.create_job_from_response(
            gfbio_response,
            &request.xml_url,
            &transformation_id,
            &filename,
        );

        let job_id = job.job_id.clone();
        let result_file = job.result_file.clone();

        self.fetch_result_data(&job_id, &result_file)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "result_fetch_error".to_string(),
                        message: format!("Error fetching result data: {}", e),
                    }),
                )
            })?;

        Ok(Json(JobResponse { job }))
    }

    pub async fn handle_transformation(
        &self,
        hook: Hook,
        // mut multipart: Multipart,
        // params: TransformationParams,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        let Some(download_url) = hook.download else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "empty_file".to_string(),
                    message: "XML file is empty".to_string(),
                }),
            ));
        };
        let filename = match hook.object {
            Resource::Project(r) => r.name,
            Resource::Collection(r) => r.name,
            Resource::Dataset(r) => r.name,
            Resource::Object(r) => r.name,
        };
        // TODO: Where to get this from?
        let transformation_id = String::new();

        let gfbio_response = match self
            .send_gfbio_request(&download_url, &transformation_id)
            .await
        {
            Ok(response) => Some(response),
            Err(e) => {
                eprintln!("GFBio API error: {}", e);
                None
            }
        };

        let job = self.create_job_from_response(
            gfbio_response,
            &download_url,
            &transformation_id,
            &filename,
        );

        Ok(Json(JobResponse { job }))
    }

    pub async fn get_job_status(
        &self,
        job_id: &str,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        //TODO implement actual job status retrieval logic
        let now = chrono::Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap());
        let job = Job {
            job_id: job_id.to_string(),
            transformation_id: "5".to_string(),
            version_id: "1".to_string(),
            input_file_url: "temp/example.xml".to_string(),
            input_file_zipped: "false".to_string(),
            query: "https://transformation.gfbio.org/api/transform?transformation=5".to_string(),
            input_file: "input/example".to_string(),
            status: "complete".to_string(),
            start_time: now - chrono::Duration::minutes(5),
            result_file: "output/result.json".to_string(),
            finish_time: Some(now),
            combined_download: format!("{}.zip", job_id),
            job_expiration_date: now + chrono::Duration::days(1),
        };

        Ok(Json(JobResponse { job }))
    }
}
