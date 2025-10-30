use crate::job::Job;
use crate::models::{ClientInterceptor, ErrorResponse, Hook, JobResponse, TransformationParams};
use aruna_rust_api::api::hooks::services::v2::hook_callback_request::Status;
use aruna_rust_api::api::hooks::services::v2::hooks_service_client::HooksServiceClient;
use aruna_rust_api::api::hooks::services::v2::{Finished, HookCallbackRequest};
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::relation::Relation as RelationEnum;
use aruna_rust_api::api::storage::models::v2::{
    InternalRelation, InternalRelationVariant, KeyValue, KeyValueVariant, Relation,
    RelationDirection, ResourceVariant,
};
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use axum::Json;
use axum::extract::Multipart;
use axum::http::StatusCode;
use chrono::FixedOffset;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::Value;
use std::error::Error;
use tokio::fs;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use urlencoding::{decode, encode};
use uuid::Uuid;

pub const CHUNK_SIZE: usize = 10_485_760;
pub const MULTIPART_THRESHOLD: usize = 104_857_600;

pub struct GfbioWebhook {
    client: Client,
    gfbio_base_url: String,
    temp_dir: String,
    transformation_id: String,
    channel: Channel,
}

impl GfbioWebhook {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url: "https://transformation.gfbio.org/api".to_string(),
            temp_dir: "./temp".to_string(),
            transformation_id: "5".to_string(),
            channel,
        }
    }

    pub fn with_config(
        transformation_id: String,
        gfbio_base_url: String,
        temp_dir: String,
        channel: Channel,
    ) -> Self {
        Self {
            client: Client::new(),
            gfbio_base_url,
            temp_dir,
            transformation_id,
            channel,
        }
    }

    async fn send_hook_callback(
        &self,
        hook: &Hook,
        status: Status,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let callback_request = HookCallbackRequest {
            secret: hook.secret.clone(),
            hook_id: hook.hook_id.clone(),
            object_id: match &hook.object {
                Resource::Object(r) => r.id.clone(),
                Resource::Dataset(r) => r.id.clone(),
                Resource::Collection(r) => r.id.clone(),
                Resource::Project(r) => r.id.clone(),
            },
            pubkey_serial: hook.pubkey_serial,
            status: Some(status),
            ..Default::default()
        };

        let interceptor = ClientInterceptor {
            api_token: hook.token.clone(),
        };

        let mut hook_client =
            HooksServiceClient::with_interceptor(self.channel.clone(), interceptor);

        let request = tonic::Request::new(callback_request);
        match hook_client.hook_callback(request).await {
            Ok(response) => {
                info!("Hook callback successfully sent: {:?}", response);
                Ok(())
            }
            Err(e) => {
                error!("Error sending hook callback: {}", e);
                Err(format!("Failed to send hook callback: {}", e).into())
            }
        }
    }

    async fn send_error_callback(
        &self,
        hook: &Hook,
        error_message: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        warn!("Sending error callback: {}", error_message);

        let callback_request = HookCallbackRequest {
            secret: hook.secret.clone(),
            hook_id: hook.hook_id.clone(),
            object_id: match &hook.object {
                Resource::Object(r) => r.id.clone(),
                Resource::Dataset(r) => r.id.clone(),
                Resource::Collection(r) => r.id.clone(),
                Resource::Project(r) => r.id.clone(),
            },
            pubkey_serial: hook.pubkey_serial,
            status: Some(Status::Finished(Finished {
                add_key_values: vec![KeyValue {
                    key: "Error".to_string(),
                    value: error_message.clone(),
                    variant: KeyValueVariant::Label as i32,
                }],
                remove_key_values: vec![],
            })),
            ..Default::default()
        };

        let interceptor = ClientInterceptor {
            api_token: hook.token.clone(),
        };

        let mut hook_client =
            HooksServiceClient::with_interceptor(self.channel.clone(), interceptor);

        let request = tonic::Request::new(callback_request);
        match hook_client.hook_callback(request).await {
            Ok(response) => {
                info!("Error callback successfully sent: {:?}", response);
                Ok(())
            }
            Err(e) => {
                error!("Error sending error callback: {}", e);
                Err(format!("Failed to send error callback: {}", e).into())
            }
        }
    }

    async fn send_success_callback(
        &self,
        hook: &Hook,
        object_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Sending success callback for object: {}", object_id);

        let status = Status::Finished(Finished {
            add_key_values: vec![KeyValue {
                key: "TRANSFORMED_BY_GFBIO".to_string(),
                value: "success".to_string(),
                variant: KeyValueVariant::Label as i32,
            }],
            remove_key_values: vec![],
        });

        self.send_hook_callback(hook, status).await
    }

    async fn save_temp_file(
        &self,
        filename: &str,
        content: &[u8],
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        fs::create_dir_all(&self.temp_dir).await?;

        let file_id = Uuid::new_v4().to_string();
        let temp_filename = format!("{}_{}", file_id, filename);
        let temp_path = format!("{}/{}", self.temp_dir, temp_filename);

        fs::write(&temp_path, content).await?;

        Ok(format!("file://{}", temp_path))
    }

    async fn send_gfbio_request(
        &self,
        input_file_url: &str,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let decoded_input_file_url = decode(input_file_url).unwrap();
        debug!("Input File URL: {:?}", input_file_url);
        debug!("Decoded input file URL: {:?}", decoded_input_file_url);

        let encoded_input_file_url = encode(decoded_input_file_url.as_ref());

        debug!("New input file URL: {:?}", encoded_input_file_url);

        let query_url = format!(
            "{}/transform?transformation={}&version=2&input_file_url={}",
            self.gfbio_base_url, self.transformation_id, encoded_input_file_url
        );

        info!("Using Test URL: {:?}", encoded_input_file_url);

        info!("Sending request to GFBio API: {:?}", query_url);

        let response = self
            .client
            .get(&query_url)
            .header("User-Agent", "GFBio-Webhook/1.0")
            .send()
            .await?;

        if response.status().is_success() {
            let json_response: Value = response.json().await?;
            Ok(json_response)
        } else {
            error!("GFBio API error: {}", response.status());
            Err(format!("GFBio API error: {}", response.status()).into())
        }
    }

    async fn fetch_result_data_and_upload(
        &self,
        hook: &Hook,
        job_id: &str,
        result_file: &str,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let url = format!("{}/results/{}/{}", self.gfbio_base_url, job_id, result_file);

        info!("Fetching result from: {}", url);

        let response = self
            .client
            .get(&url)
            .header("User-Agent", "GFBio-Webhook/1.0")
            .send()
            .await?;

        if !response.status().is_success() {
            error!("Failed to fetch result data: {}", response.status());
            return Err(format!("Failed to fetch result data: {}", response.status()).into());
        }

        debug!("Successfully fetched result data from GFBio");

        let creds = Credentials::new(
            hook.access_key
                .clone()
                .ok_or_else(|| "Got no access key from hook".to_string())?,
            hook.secret_key
                .clone()
                .ok_or_else(|| "Got no secret key from hook".to_string())?,
            None,
            None,
            "ARUNA_SERVER",
        );

        debug!("Using provided credentials to upload result data to S3-compatible storage");

        let config = aws_config::defaults(BehaviorVersion::v2025_08_07())
            .credentials_provider(creds)
            .load()
            .await;

        let dlurl = reqwest::Url::parse(
            &hook
                .download
                .clone()
                .ok_or_else(|| "Got no download url from hook".to_string())?,
        )?;

        debug!("Parsed download URL: {:?}", dlurl);

        let origin = dlurl.origin().unicode_serialization();
        let host = dlurl
            .host()
            .map(|h| h.to_string())
            .ok_or_else(|| "Invalid presigned download url".to_string())?;
        let (bucket, cleaned_host) = host
            .split_once('.')
            .ok_or_else(|| "No bucket set in host".to_string())?;

        debug!("Origin: {:?}", origin);
        debug!("Host: {:?}", host);
        debug!("Cleaned host: {:?}", cleaned_host);

        let endpoint_url = match dlurl.port() {
            Some(port) => format!("{}://{}:{}", dlurl.scheme(), cleaned_host, port),
            None => format!("{}://{}", dlurl.scheme(), cleaned_host),
        };

        debug!("Using endpoint URL for S3 upload: {:?}", endpoint_url);

        let Some(key) = dlurl.path().strip_prefix("/") else {
            return Err("Invalid path in presigned download url".to_string().into());
        };

        let new_key = key.replace(".xml", ".json");

        debug!(
            "S3 Upload Target:\n\tBucket: {}\n\tKey: {}",
            bucket, new_key
        );

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(endpoint_url.to_string())
            .build();

        debug!("S3 config: {:?}", s3_config);

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        debug!("S3 client successfully created, beginning upload...");

        let (multipart, trigger_object) = match &hook.object {
            Resource::Object(r) => (r.content_len as usize >= MULTIPART_THRESHOLD, r.id.clone()),
            _ => {
                return Err("Invalid hook triggered: not an object resource"
                    .to_string()
                    .into());
            }
        };

        // -----------------------------------------
        // UPLOAD SECTION
        // -----------------------------------------

        debug!(
            "Starting S3 upload (multipart: {}) for job_id = {} and result_file = {}",
            multipart, job_id, result_file
        );

        // Create Object

        let object = &hook.object;

        let orig_desc = match object {
            Resource::Object(r) => r.description.clone(),
            Resource::Dataset(r) => r.description.clone(),
            Resource::Collection(r) => r.description.clone(),
            Resource::Project(r) => r.description.clone(),
        };

        let new_desc = format!(
            "This object represents a biological collection entity derived from an ABCD record and transformed into a BioSchema-compliant format.\
                                            \n\nOriginal description of ABCD file:\n{}",
            orig_desc
        );

        let orig_key_values = match object {
            Resource::Object(r) => r.key_values.clone(),
            Resource::Dataset(r) => r.key_values.clone(),
            Resource::Collection(r) => r.key_values.clone(),
            Resource::Project(r) => r.key_values.clone(),
        };

        let filtered_key_values: Vec<KeyValue> = orig_key_values
            .into_iter()
            .filter(|kv| kv.key != "ABCD")
            .collect();

        let bioschema_key_values = vec![
            KeyValue {
                key: "TRANSFORMED_BY_GFBIO".to_string(),
                value: "success".to_string(),
                variant: KeyValueVariant::Label as i32,
            },
            KeyValue {
                key: "BioSchema".to_string(),
                value: "true".to_string(),
                variant: KeyValueVariant::Label as i32,
            },
        ];

        let new_key_values: Vec<KeyValue> = filtered_key_values
            .into_iter()
            .chain(bioschema_key_values)
            .collect();

        let request = CreateObjectRequest {
            name: new_key.to_string(),
            title: match object {
                Resource::Object(r) => format!("{} BioSchema", r.title.clone()),
                Resource::Dataset(r) => format!("{} BioSchema", r.title.clone()),
                Resource::Collection(r) => format!("{} BioSchema", r.title.clone()),
                Resource::Project(r) => format!("{} BioSchema", r.title.clone()),
            },
            description: new_desc,
            key_values: new_key_values,
            relations: vec![Relation {
                relation: Some(RelationEnum::Internal(InternalRelation {
                    resource_id: trigger_object.clone(),
                    resource_variant: ResourceVariant::Object as i32,
                    defined_variant: InternalRelationVariant::Origin as i32,
                    custom_variant: None,
                    direction: RelationDirection::Outbound as i32,
                })),
            }],
            data_class: match object {
                Resource::Object(r) => r.data_class,
                Resource::Dataset(r) => r.data_class,
                Resource::Collection(r) => r.data_class,
                Resource::Project(r) => r.data_class,
            },
            hashes: vec![],
            metadata_license_tag: match object {
                Resource::Object(r) => r.metadata_license_tag.clone(),
                Resource::Dataset(r) => r.metadata_license_tag.clone(),
                Resource::Collection(r) => r.metadata_license_tag.clone(),
                Resource::Project(r) => r.metadata_license_tag.clone(),
            },
            data_license_tag: match object {
                Resource::Object(r) => r.data_license_tag.clone(),
                _ => "CC-BY-4.0".to_string(),
            },
            parent: match object {
                Resource::Object(r) => r.relations.clone().iter().find_map(|rel| {
                    if let Some(RelationEnum::Internal(internal)) = &rel.relation {
                        if internal.direction == RelationDirection::Inbound as i32 {
                            match internal.resource_variant {
                                v if v == ResourceVariant::Project as i32 => {
                                    Some(Parent::ProjectId(internal.resource_id.clone()))
                                }
                                v if v == ResourceVariant::Collection as i32 => {
                                    Some(Parent::CollectionId(internal.resource_id.clone()))
                                }
                                v if v == ResourceVariant::Dataset as i32 => {
                                    Some(Parent::DatasetId(internal.resource_id.clone()))
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }),
                Resource::Dataset(_) => None,
                Resource::Collection(_) => None,
                Resource::Project(_) => None,
            },
            authors: match object {
                Resource::Object(r) => r.authors.clone(),
                Resource::Dataset(r) => r.authors.clone(),
                Resource::Collection(r) => r.authors.clone(),
                Resource::Project(r) => r.authors.clone(),
            },
        };

        debug!("Requesting object creation: {:#?}", request);

        let interceptor = ClientInterceptor {
            api_token: hook.token.clone(),
        };

        let mut client =
            aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient::with_interceptor(
                self.channel.clone(),
                interceptor.clone()
            );

        // Send the request to the Aruna instance gRPC endpoint
        match client.create_object(request).await {
            Ok(_) => {
                debug!("Object creation request sent successfully");
            }
            Err(e) => {
                error!("Failed to send object creation request: {:?}", e);
            }
        }

        let etag = if multipart {
            debug!("Creating multipart upload for key = {}", new_key);
            let upload_id = match s3_client
                .create_multipart_upload()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(new_key.to_string()))
                .send()
                .await
            {
                Ok(resp) => {
                    let id = resp.upload_id().map(|s| s.to_string()).unwrap_or_default();
                    debug!("Multipart upload created: upload_id = {}", id);
                    id
                }
                Err(e) => {
                    error!("Failed to create multipart upload: {:?}", e);
                    return Err(format!("Failed to create multipart upload: {}", e).into());
                }
            };

            let mut stream = response.bytes_stream().chunks(CHUNK_SIZE).into_inner();
            let mut builder = CompletedMultipartUpload::builder();
            let mut counter = 0;

            while let Some(bytes) = stream.next().await {
                counter += 1;
                debug!("Uploading part {}...", counter);

                match bytes {
                    Ok(chunked_stream) => {
                        match s3_client
                            .upload_part()
                            .set_bucket(Some(bucket.to_string()))
                            .set_key(Some(new_key.to_string()))
                            .body(chunked_stream.into())
                            .upload_id(&upload_id)
                            .part_number(counter)
                            .send()
                            .await
                        {
                            Ok(part) => {
                                debug!(
                                    "Part {} uploaded successfully (etag = {:?})",
                                    counter,
                                    part.e_tag()
                                );
                                builder = builder.parts(
                                    CompletedPart::builder()
                                        .e_tag(part.e_tag().unwrap_or_default())
                                        .part_number(counter)
                                        .build(),
                                );
                            }
                            Err(e) => {
                                error!("Failed to upload part {}: {:?}", counter, e);
                                return Err(
                                    format!("Failed to upload part {}: {}", counter, e).into()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error reading chunk {}: {:?}", counter, e);
                        return Err(format!("Error reading chunk {}: {}", counter, e).into());
                    }
                }
            }

            debug!("All parts uploaded successfully, completing multipart upload...");

            match s3_client
                .complete_multipart_upload()
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(new_key.to_string()))
                .upload_id(&upload_id)
                .multipart_upload(builder.build())
                .send()
                .await
            {
                Ok(resp) => {
                    debug!("Multipart upload completed successfully");
                    resp.e_tag
                }
                Err(e) => {
                    error!("Failed to complete multipart upload: {:?}", e);
                    return Err(format!("Failed to complete multipart upload: {}", e).into());
                }
            }
        } else {
            debug!("Performing single PUT upload for key = {}", new_key);

            match s3_client
                .put_object()
                .body(response.bytes().await?.into())
                .set_bucket(Some(bucket.to_string()))
                .set_key(Some(new_key.to_string()))
                .send()
                .await
            {
                Ok(resp) => {
                    debug!("Single PUT upload completed successfully");
                    resp.e_tag
                }
                Err(e) => {
                    error!("Failed to upload object via single PUT: {:?}", e);
                    return Err(format!("Failed to upload object via single PUT: {}", e).into());
                }
            }
        };

        // -----------------------------------------
        // END UPLOAD SECTION
        // -----------------------------------------

        debug!("S3 upload completed, etag = {:?}", etag);

        let object_id = etag
            .ok_or_else(|| "No etag returned".to_string())?
            .strip_prefix("-")
            .map(|p| p.to_string())
            .ok_or_else(|| "Invalid etag provided".to_string())?;

        debug!("Using etag-derived object_id={}", object_id);
        Ok(object_id)
    }

    fn create_job_from_response(
        &self,
        gfbio_response: Option<Value>,
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
                error!("GFBio API error: {}", e);
                None
            }
        };

        let job = self.create_job_from_response(
            gfbio_response,
            &temp_file_url,
            &transformation_id,
            &filename,
        );

        debug!("Job from GFBio Response: {:?}", job);

        Ok(Json(JobResponse { job }))
    }

    pub async fn handle_url_transformation(
        &self,
        hook: Hook,
    ) -> Result<Json<JobResponse>, (StatusCode, Json<ErrorResponse>)> {
        info!("Received hook: {:?}", hook);

        let Some(download_url) = hook.download.clone() else {
            let error_msg = "No download URL provided in hook".to_string();
            error!("{}", error_msg);

            if let Err(e) = self.send_error_callback(&hook, error_msg.clone()).await {
                error!("Failed to send error callback: {}", e);
            }

            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "missing_download_url".to_string(),
                    message: error_msg,
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
            Ok(response) => {
                info!("GFBio response: {:?}", response);
                Some(response)
            }
            Err(e) => {
                let error_msg = format!("GFBio API request failed: {}", e);
                error!("{}", error_msg);

                if let Err(callback_err) = self.send_error_callback(&hook, error_msg.clone()).await
                {
                    error!("Failed to send error callback: {}", callback_err);
                }

                return Err((
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: "gfbio_api_error".to_string(),
                        message: error_msg,
                    }),
                ));
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

        info!(
            "Fetching result data for\n\tjob_id: {:?}\n\tresult_file: {:?}",
            job_id, result_file
        );

        match self
            .fetch_result_data_and_upload(&hook, &job_id, &result_file)
            .await
        {
            Ok(object_id) => {
                info!(
                    "Successfully fetched and uploaded result data. Object ID: {}",
                    object_id
                );

                if let Err(e) = self.send_success_callback(&hook, object_id).await {
                    error!("Failed to send success callback: {}", e);
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "callback_error".to_string(),
                            message: format!(
                                "Transformation successful but callback failed: {}",
                                e
                            ),
                        }),
                    ));
                }

                Ok(Json(JobResponse { job }))
            }
            Err(e) => {
                let error_msg = format!("Failed to fetch or upload result data: {}", e);
                error!("{}", error_msg);

                if let Err(callback_err) = self.send_error_callback(&hook, error_msg.clone()).await
                {
                    error!("Failed to send error callback: {}", callback_err);
                }

                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "result_fetch_error".to_string(),
                        message: error_msg,
                    }),
                ))
            }
        }
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
