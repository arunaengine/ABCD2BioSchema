use crate::job::Job;
use crate::models::{ErrorResponse, Hook, JobResponse, TransformationParams};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
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
    transformation_id: String,
}

impl GfbioWebhook {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url: "https://transformation.gfbio.org/api".to_string(),
            temp_dir: "./temp".to_string(),
            transformation_id: "5".to_string(),
        }
    }

    pub fn with_config(
        transformation_id: String,
        gfbio_base_url: String,
        temp_dir: String,
    ) -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url,
            temp_dir,
            transformation_id,
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
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let query_url = format!(
            "{}/transform?transformation={}&input_file_url={}&input_file_zipped=false",
            self.gfbio_base_url, self.transformation_id, input_file_url
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
        hook: Hook,
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

        if !response.status().is_success() {
            return Err(format!("Failed to fetch result data: {}", response.status()).into());
        }

        let creds = Credentials::new(
            hook.access_key
                .ok_or_else(|| format!("Got no access key from hook"))?,
            hook.secret_key
                .ok_or_else(|| format!("Got no secret key from hook"))?,
            None,
            None,
            "ARUNA_SERVER", // Endpoint name?
        );
        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .credentials_provider(creds)
            .load()
            .await;

        // TODO: Parse from download url
        let dlurl = reqwest::Url::parse(
            &hook
                .download
                .ok_or_else(|| format!("Got no download url from hook"))?,
        )?;
        let origin = dlurl.origin().unicode_serialization();
        let host = dlurl
            .host()
            .ok_or_else(|| format!("Invalid presigned download url"))?;

        let endpoint_url = match dlurl.port() {
            Some(port) => format!("{}://{}:{}", origin, host, port),
            None => format!("{}://{}", origin, host),
        };
        let Some(path) = dlurl.path().strip_prefix("/") else {
            return Err(format!("Invalid path in presigned download url").into());
        };
        let (bucket, key) = path
            .split_once('/')
            .ok_or_else(|| format!("Invalid path in presigned download url"))?;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(endpoint_url.to_string())
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let multipart = match hook.object {
            Resource::Object(r) => r.content_len >= 104_857_600,
            _ => {
                return Err(format!("Invalid hook triggered").into());
            }
        };

        if multipart {
            let upload_id = s3_client
                .create_multipart_upload()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(key.to_string()))
                .send()
                .await?
                .upload_id()
                .map(|id| id.to_string());

            let stream = response.bytes_stream();
            todo!("Impl multipart")

        } else {
            let upload_id = s3_client
                .put_object()
                .body(response.bytes().await?.into())
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(key.to_string()))
                .send()
                .await?;
        };

        // TODO: Upload to s3
        // self
        // .s3_client
        // .put_object()
        // .set_bucket(Some(location.bucket))
        // .set_key(Some(location.key))
        // .set_content_length(Some(content_len))
        // .body(bytestream)
        // .send()
        // .await
        //
        let json_response: serde_json::Value = todo!();
        //response.json().await?;
        //println!("Result data fetched successfully:\n{:#?}", json_response);
        Ok(json_response)
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

    // pub async fn handle_url_transformation(
    //     &self,
    //     request: TransformationRequest,
    // ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
    //     let transformation_id = request.transformation_id.unwrap_or_else(|| "5".to_string());

    //     if !request.xml_url.starts_with("http") {
    //         return Err((
    //             StatusCode::BAD_REQUEST,
    //             Json(ErrorResponse {
    //                 error: "invalid_url".to_string(),
    //                 message: "URL must start with http or https".to_string(),
    //             }),
    //         ));
    //     }

    //     let xml_content = self.download_xml(&request.xml_url).await.map_err(|e| {
    //         (
    //             StatusCode::BAD_REQUEST,
    //             Json(ErrorResponse {
    //                 error: "download_error".to_string(),
    //                 message: format!("Failed to download XML: {}", e),
    //             }),
    //         )
    //     })?;

    //     if xml_content.is_empty() {
    //         return Err((
    //             StatusCode::BAD_REQUEST,
    //             Json(ErrorResponse {
    //                 error: "empty_file".to_string(),
    //                 message: "Downloaded XML is empty".to_string(),
    //             }),
    //         ));
    //     }

    //     let filename = request
    //         .xml_url
    //         .split('/')
    //         .last()
    //         .unwrap_or("downloaded.xml")
    //         .to_string();

    //     let gfbio_response = match self
    //         .send_gfbio_request(&request.xml_url, &transformation_id)
    //         .await
    //     {
    //         Ok(response) => Some(response),
    //         Err(e) => {
    //             eprintln!("GFBio API error: {}", e);
    //             None
    //         }
    //     };

    //     let job = self.create_job_from_response(
    //         gfbio_response,
    //         &request.xml_url,
    //         &transformation_id,
    //         &filename,
    //     );

    //     let job_id = job.job_id.clone();
    //     let result_file = job.result_file.clone();

    //     self.fetch_result_data(&job_id, &result_file)
    //         .await
    //         .map_err(|e| {
    //             (
    //                 StatusCode::INTERNAL_SERVER_ERROR,
    //                 Json(ErrorResponse {
    //                     error: "result_fetch_error".to_string(),
    //                     message: format!("Error fetching result data: {}", e),
    //                 }),
    //             )
    //         })?;

    //     Ok(Json(JobResponse { job }))
    // }

    pub async fn handle_transformation(
        &self,
        mut multipart: Multipart,
        params: TransformationParams,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        let transformation_id = params.transformation_id.unwrap_or_else(|| "5".to_string());

        let mut xml_content: Option<(String, Vec<u8>)> = None;

        while let Some(field) = multipart.next_field().await.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "multipart_error".to_string(),
                    message: format!("Error reading multipart data: {}", e),
                }),
            )
        })? {
            if field.name() == Some("xml_file") {
                let filename = field.file_name().unwrap_or("upload.xml").to_string();
                let content = field.bytes().await.map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: "file_read_error".to_string(),
                            message: format!("Error reading file: {}", e),
                        }),
                    )
                })?;

                xml_content = Some((filename, content.to_vec()));
                break;
            }
        }

        let (filename, content) = xml_content.ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "missing_file".to_string(),
                    message: "No XML file provided".to_string(),
                }),
            )
        })?;

        if content.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "empty_file".to_string(),
                    message: "XML file is empty".to_string(),
                }),
            ));
        }

        let temp_file_url = self
            .save_temp_file(&filename, &content)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "file_save_error".to_string(),
                        message: format!("Error saving temporary file: {}", e),
                    }),
                )
            })?;

        let gfbio_response = match self.send_gfbio_request(&temp_file_url).await {
            Ok(response) => Some(response),
            Err(e) => {
                eprintln!("GFBio API error: {}", e);
                None
            }
        };

        let job = self.create_job_from_response(
            gfbio_response,
            &temp_file_url,
            &transformation_id,
            &filename,
        );

        Ok(Json(JobResponse { job }))
    }

    pub async fn handle_url_transformation(
        &self,
        hook: Hook,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        let Some(download_url) = hook.download.clone() else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "empty_file".to_string(),
                    message: "XML file is empty".to_string(),
                }),
            ));
        };
        let filename = match hook.object.clone() {
            Resource::Project(r) => r.name,
            Resource::Collection(r) => r.name,
            Resource::Dataset(r) => r.name,
            Resource::Object(r) => r.name,
        };

        let gfbio_response = match self.send_gfbio_request(&download_url).await {
            Ok(response) => Some(response),
            Err(e) => {
                eprintln!("GFBio API error: {}", e);
                None
            }
        };

        let job = self.create_job_from_response(
            gfbio_response,
            &download_url,
            &self.transformation_id,
            &filename,
        );

        let job_id = job.job_id.clone();
        let result_file = job.result_file.clone();

        self.fetch_result_data(hook, &job_id, &result_file)
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
