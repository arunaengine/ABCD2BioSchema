# Building Webhook Services for Aruna: A Complete Guide

This guide explains how to create services that integrate with the Aruna data orchestration platform via webhooks, using the [ABCD2BioSchema service](https://github.com/arunaengine/ABCD2BioSchema) as a practical example.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Step-by-Step Implementation](#step-by-step-implementation)
5. [Webhook Registration](#webhook-registration)
6. [Best Practices](#best-practices)
7. [Testing & Debugging](#testing--debugging)
8. [Deployment](#deployment)

---

## Overview

### What are Aruna Webhooks?

Aruna webhooks allow you to automate workflows by triggering external services when specific events occur in your Aruna engine. Common use cases include:

- Data transformation and processing
- Format conversion
- Metadata extraction
- Quality validation
- Notification systems

### How They Work

```
┌─────────────┐                    ┌──────────────────┐
│             │   1. Event occurs  │                  │
│   Aruna     │───────────────────►│  Your Webhook    │
│   Engine    │                    │     Service      │
│             │   2. Hook payload  │                  │
│             │◄───────────────────┤                  │
│             │                    │  3. Processing   │
│             │   4. Callback      │                  │
│             │◄───────────────────┤                  │
└─────────────┘                    └──────────────────┘
```

**Flow:**
1. User performs action in Aruna (e.g., uploads a file)
2. Aruna triggers your webhook with payload
3. Your service processes the request
4. Service sends a callback to Aruna with results

---

## Prerequisites

### Technical Requirements

- **HTTP Server**: Ability to receive POST requests
- **gRPC Client**: For communication with Aruna API
- **Network Access**: Your service must be reachable from Aruna
- **Authentication**: Valid Aruna API token

### Knowledge Requirements

- Understanding of HTTP/REST APIs
- Basic knowledge of gRPC (for callbacks)
- Familiarity with asynchronous processing
- Error handling patterns

---

## Architecture

### Components Overview

```
Your Webhook Service
├── HTTP Server (Axum/Express/Flask/etc.)
│   └── POST /your-endpoint
├── gRPC Client (Aruna API)
│   ├── Object Operations
│   └── Hook Callbacks
├── Business Logic
│   └── Your Processing
└── External APIs (optional)
    └── Third-party services
```

### Data Flow in ABCD2BioSchema Example

```
┌────────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────────┐
│   Aruna    │────►│   Service   │────►│  GFBio   │────►│    Aruna    │
│  (Source)  │     │  (Process)  │     │   API    │     │   (Result)  │
└────────────┘     └─────────────┘     └──────────┘     └─────────────┘
     │                    │                   │                  │
     │ 1. Hook trigger    │                   │                  │
     │───────────────────►│                   │                  │
     │                    │ 2. Transform req  │                  │
     │                    │──────────────────►│                  │
     │                    │ 3. JSON result    │                  │
     │                    │◄──────────────────│                  │
     │                    │          4. Upload object            │
     │                    │─────────────────────────────────────►│
     │  5. Send callback  │                   │                  │
     │◄───────────────────│                   │                  │
```

---

## Step-by-Step Implementation

### Step 1: Define Your Data Models

First, create structures to handle webhook payloads from Aruna.

```rust
// models.rs
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Hook {
    // Webhook identification
    pub hook_id: String,
    pub secret: String,
    
    // Object information
    pub object: Resource,
    
    // Download credentials
    pub download: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    
    // API authentication
    pub token: String,
    pub pubkey_serial: i32,
}
```

**Key Fields Explained:**

- `hook_id`: Unique identifier for this webhook trigger
- `secret`: Verification secret for the webhook
- `object`: Complete Aruna resource (Object/Dataset/Collection/Project)
- `download`: Presigned S3 URL to download the file
- `access_key` / `secret_key`: Temporary S3 credentials for uploads
- `token`: Bearer token for Aruna API authentication
- `pubkey_serial`: Public key serial for verification

### Step 2: Create HTTP Endpoint

Set up an endpoint to receive webhook requests.

```rust
// service.rs
use axum::{Json, extract::State};
use std::sync::Arc;

pub async fn webhook_handler(
    State(state): State<Arc<Handler>>,
    Json(hook): Json<Hook>,
) -> Result<Json<Response>, (StatusCode, Json<ErrorResponse>)> {
    info!("Received webhook: hook_id={}", hook.hook_id);
    
    // Validate hook payload
    if hook.download.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "missing_download_url".to_string(),
                message: "No download URL provided".to_string(),
            }),
        ));
    }
    
    // Process the webhook
    state.processor.handle_webhook(hook).await
}
```

### Step 3: Implement gRPC Client for Callbacks

Create a client to communicate back with Aruna.

```rust
// webhook.rs
use aruna_rust_api::api::hooks::services::v2::hooks_service_client::HooksServiceClient;
use aruna_rust_api::api::hooks::services::v2::{HookCallbackRequest, Finished};
use tonic::transport::Channel;

pub struct WebhookService {
    channel: Channel,
}

impl WebhookService {
    pub async fn send_callback(
        &self,
        hook: &Hook,
        status: Status,
    ) -> Result<(), Box<dyn Error>> {
        // Prepare callback request
        let s3_client = S3Client::from_conf(
            aws_sdk_s3::config::Builder::from(&config).build()
        );
        
        // Get original key and modify for a new file
        let original_key = url.path().strip_prefix("/").unwrap();
        let new_key = original_key.replace(".xml", ".json");
        
        // Upload to S3
        let response = s3_client
            .put_object()
            .bucket(bucket)
            .key(&new_key)
            .body(data.into())
            .send()
            .await?;
        
        // Extract object ID from ETag
        let object_id = response.e_tag()
            .unwrap()
            .strip_prefix("-")
            .unwrap()
            .to_string();
        
        Ok(object_id)
    }
}
```

**For Large Files (Multipart Upload):**

```rust
const CHUNK_SIZE: usize = 10_485_760;  // 10 MB
const MULTIPART_THRESHOLD: usize = 104_857_600;  // 100 MB

async fn upload_large_file(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
) -> Result<String, Box<dyn Error>> {
    // Initiate multipart upload
    let upload = s3_client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    
    let upload_id = upload.upload_id().unwrap();
    
    // Upload parts
    let mut parts = Vec::new();
    for (i, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
        let part_number = (i + 1) as i32;
        
        let part = s3_client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(chunk.to_vec().into())
            .send()
            .await?;
        
        parts.push(
            CompletedPart::builder()
                .e_tag(part.e_tag().unwrap())
                .part_number(part_number)
                .build()
        );
    }
    
    // Complete upload
    let completed = s3_client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build()
        )
        .send()
        .await?;
    
    Ok(completed.e_tag().unwrap().to_string())
}
```

### Step 7: Create a New Object in Aruna

Register the uploaded file as a new object with metadata.

```rust
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient;

impl WebhookService {
    async fn create_object(
        &self,
        hook: &Hook,
        new_filename: &str,
        trigger_object_id: &str,
    ) -> Result<(), Box<dyn Error>> {
        let request = CreateObjectRequest {
            name: new_filename.to_string(),
            title: format!("{} (Processed)", get_title(&hook.object)),
            description: format!(
                "Processed result from {}",
                get_description(&hook.object)
            ),
            
            // Add labels
            key_values: vec![
                KeyValue {
                    key: "PROCESSED".to_string(),
                    value: "true".to_string(),
                    variant: KeyValueVariant::Label as i32,
                },
            ],
            
            // Link to an original object
            relations: vec![
                Relation {
                    relation: Some(RelationEnum::Internal(
                        InternalRelation {
                            resource_id: trigger_object_id.to_string(),
                            resource_variant: ResourceVariant::Object as i32,
                            defined_variant: InternalRelationVariant::Origin as i32,
                            direction: RelationDirection::Outbound as i32,
                            custom_variant: None,
                        }
                    )),
                }
            ],
            
            // Inherit properties
            data_class: get_data_class(&hook.object),
            metadata_license_tag: get_metadata_license(&hook.object),
            data_license_tag: get_data_license(&hook.object),
            authors: get_authors(&hook.object),
            parent: get_parent(&hook.object),
            hashes: vec![],
        };
        
        let interceptor = ClientInterceptor {
            api_token: hook.token.clone(),
        };
        
        let mut client = ObjectServiceClient::with_interceptor(
            self.channel.clone(),
            interceptor
        );
        
        client.create_object(request).await?;
        Ok(())
    }
}
```

### Step 8: Send Success/Error Callbacks

Always notify Aruna of the processing result.

```rust
impl WebhookService {
    async fn send_success_callback(
        &self,
        hook: &Hook,
        object_id: String,
    ) -> Result<(), Box<dyn Error>> {
        let status = Status::Finished(Finished {
            add_key_values: vec![
                KeyValue {
                    key: "PROCESSING_STATUS".to_string(),
                    value: "success".to_string(),
                    variant: KeyValueVariant::Label as i32,
                },
            ],
            remove_key_values: vec![],
        });
        
        self.send_callback(hook, status).await
    }
    
    async fn send_error_callback(
        &self,
        hook: &Hook,
        error_msg: String,
    ) -> Result<(), Box<dyn Error>> {
        let status = Status::Finished(Finished {
            add_key_values: vec![
                KeyValue {
                    key: "ERROR".to_string(),
                    value: error_msg.clone(),
                    variant: KeyValueVariant::Label as i32,
                },
            ],
            remove_key_values: vec![],
        });
        
        self.send_callback(hook, status).await
    }
}
```

### Step 9: Complete Request Handler

Combine all steps into a complete handler.

```rust
impl WebhookService {
    pub async fn handle_webhook(
        &self,
        hook: Hook,
    ) -> Result<Json<Response>, (StatusCode, Json<ErrorResponse>)> {
        info!("Processing webhook: {}", hook.hook_id);
        
        // Step 1: Download a source file
        let data = match self.download_file(
            hook.download.as_ref().unwrap()
        ).await {
            Ok(d) => d,
            Err(e) => {
                error!("Download failed: {}", e);
                let _ = self.send_error_callback(&hook, e.to_string()).await;
                return Err((
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: "download_failed".to_string(),
                        message: e.to_string(),
                    }),
                ));
            }
        };
        
        // Step 2: Process data
        let processed = match self.process_data(data).await {
            Ok(p) => p,
            Err(e) => {
                error!("Processing failed: {}", e);
                let _ = self.send_error_callback(&hook, e.to_string()).await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "processing_failed".to_string(),
                        message: e.to_string(),
                    }),
                ));
            }
        };
        
        // Step 3: Upload result
        let object_id = match self.upload_result(
            &hook, 
            processed.data, 
            "result.json"
        ).await {
            Ok(id) => id,
            Err(e) => {
                error!("Upload failed: {}", e);
                let _ = self.send_error_callback(&hook, e.to_string()).await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "upload_failed".to_string(),
                        message: e.to_string(),
                    }),
                ));
            }
        };
        
        // Step 4: Create an object in Aruna
        if let Err(e) = self.create_object(
            &hook, 
            "result.json",
            &object_id
        ).await {
            error!("Object creation failed: {}", e);
        }
        
        // Step 5: Send success callback
        if let Err(e) = self.send_success_callback(&hook, object_id.clone()).await {
            error!("Callback failed: {}", e);
        }
        
        Ok(Json(Response {
            status: "success".to_string(),
            object_id,
        }))
    }
}
```

---

## Webhook Registration

### Step 1: Prepare Registration Script

Create a script to register your webhook with Aruna.

```bash
#!/bin/bash
# register_webhook.sh

TOKEN="your-aruna-api-token"
PROJECT="your-project-id"
TOKEN_EXPIRATION="1735689600000"  # Unix timestamp in milliseconds

curl -X 'POST' \
    'https://your-aruna-instance.com/v2/hooks' \
    -H 'accept: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "YourServiceName",
        "trigger": {
            "triggerType": "TRIGGER_TYPE_OBJECT_FINISHED",
            "filters": [
                {
                    "keyValue": {
                        "key": "^YOUR_LABEL$",
                        "value": ".*",
                        "variant": "KEY_VALUE_VARIANT_LABEL"
                    }
                }
            ]
        },
        "hook": {
            "externalHook": {
                "url": "https://your-service.com/webhook",
                "method": "METHOD_POST"
            }
        },
        "timeout": "'"${TOKEN_EXPIRATION}"'",
        "projectIds": ["'"${PROJECT}"'"],
        "description": "Your service description"
    }'
```

### Step 2: Trigger Types

[Available trigger types](https://docs.aruna-engine.org/latest/get_started/basic_usage/13_How-To-Hooks/):

| Trigger Type                       | When It Fires                                                                                            |
|------------------------------------|----------------------------------------------------------------------------------------------------------|
| `TRIGGER_TYPE_HOOK_ADDED`          | Triggers, when a Hook key-value gets added to a resource. Can be limited to specific values.             |
| `TRIGGER_TYPE_RESOURCE_CREATED`    | Triggers for all hierarchical resources on creation.                                                     |
| `TRIGGER_TYPE_LABEL_ADDED`         | Triggers, when a Label key-value gets added to a resource. Can be limited to specific values.            |
| `TRIGGER_TYPE_OBJECT_FINISHED`     | Triggers only for Objects on finish.                                                                     |
| `TRIGGER_TYPE_STATIC_LABEL_ADDED`  | Triggers, when an immutable Label key-value gets added to a resource. Can be limited to specific values. |
| `TRIGGER_TYPE_HOOK_STATUS_CHANGED` | Triggers, if a hook status change occurs on a resource.                                                  |


### Step 3: Filter Configuration

**Label Filters:**
```json
{
    "keyValue": {
        "key": "^ABCD$",           // Regex pattern for label key
        "value": ".*",              // Regex pattern for label value
        "variant": "KEY_VALUE_VARIANT_LABEL"
    }
}
```

**Name Filters:**
```json
{
    "name": {
        "name": ".*\\.xml$"         // Regex for filename
    }
}
```

**Multiple Filters (AND logic):**
```json
"filters": [
    {
        "keyValue": {
            "key": "^FORMAT$",
            "value": "^XML$",
            "variant": "KEY_VALUE_VARIANT_LABEL"
        }
    },
    {
        "name": {
            "name": ".*\\.xml$"
        }
    }
]
```

---

## Best Practices

### 1. Error Handling

**Always send callbacks, even on error:**

```rust
match process_webhook(hook).await {
    Ok(result) => {
        send_success_callback(&hook, result).await?;
    }
    Err(e) => {
        // CRITICAL: Always notify Aruna
        send_error_callback(&hook, e.to_string()).await?;
        return Err(e);
    }
}
```

### 2. Idempotency

Make your webhook handler idempotent to handle retries:

```rust
async fn handle_webhook(hook: Hook) -> Result<()> {
    // Check if already processed
    if already_processed(&hook.hook_id).await? {
        info!("Hook {} already processed, skipping", hook.hook_id);
        return Ok(());
    }
    
    // Process and mark as complete
    process(hook).await?;
    mark_processed(&hook.hook_id).await?;
    
    Ok(())
}
```

### 3. Async Processing

For long-running tasks, acknowledge immediately and process asynchronously:

```rust
pub async fn webhook_handler(
    hook: Hook,
) -> Result<Json<Response>, StatusCode> {
    // Validate and accept quickly
    validate_hook(&hook)?;
    
    // Spawn background task
    tokio::spawn(async move {
        if let Err(e) = process_hook(hook.clone()).await {
            error!("Processing failed: {}", e);
            let _ = send_error_callback(&hook, e.to_string()).await;
        }
    });
    
    // Return immediately
    Ok(Json(Response {
        status: "accepted".to_string(),
        message: "Processing started".to_string(),
    }))
}
```

### 4. Logging

Implement comprehensive logging:

```rust
info!("Webhook received: hook_id={}, object_id={}", 
      hook.hook_id, object_id);
debug!("Download URL: {}", hook.download.as_ref().unwrap());
debug!("Processing with params: {:?}", params);
info!("Upload completed: object_id={}", new_object_id);
error!("Failed to process: {}", error);
```

### 5. Security

**Verify webhook secrets:**
```rust
fn verify_webhook(hook: &Hook, expected_secret: &str) -> Result<()> {
    if hook.secret != expected_secret {
        return Err("Invalid webhook secret".into());
    }
    Ok(())
}
```

**Use HTTPS:**
```rust
let endpoint = if server_url.starts_with("https") {
    Channel::from_shared(server_url)?
        .tls_config(ClientTlsConfig::new())?
} else {
    return Err("Only HTTPS endpoints allowed".into());
};
```

### 6. Resource Management

**Clean up temporary files:**
```rust
async fn process_with_cleanup(hook: Hook) -> Result<()> {
    let temp_file = save_temp_file(&data).await?;
    
    let result = process_file(&temp_file).await;
    
    // Always cleanup, even on error
    let _ = fs::remove_file(&temp_file).await;
    
    result
}
```

### 7. Timeouts

Set appropriate timeouts:

```rust
let response = tokio::time::timeout(
    Duration::from_secs(300),  // 5-minute timeout
    process_data(data)
).await??;
```

---

## Testing & Debugging

### Local Testing

**1. Mock Aruna Webhook:**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_hook() -> Hook {
        Hook {
            hook_id: "test-hook-123".to_string(),
            secret: "test-secret".to_string(),
            object: create_test_object(),
            download: Some("https://example.com/test.xml".to_string()),
            access_key: Some("test-key".to_string()),
            secret_key: Some("test-secret".to_string()),
            token: "test-token".to_string(),
            pubkey_serial: 1,
        }
    }
    
    #[tokio::test]
    async fn test_webhook_handler() {
        let service = WebhookService::new(test_channel());
        let hook = create_test_hook();
        
        let result = service.handle_webhook(hook).await;
        assert!(result.is_ok());
    }
}
```

**2. Manual Curl Test:**

```bash
curl -X POST http://localhost:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "hook_id": "test-123",
    "secret": "test-secret",
    "object": {
      "object": {
        "id": "obj-456",
        "name": "test.xml",
        "content_len": 1024
      }
    },
    "download": "https://example.com/test.xml",
    "token": "test-token",
    "pubkey_serial": 1
  }'
```

### Debugging Checklist

- [ ] Service is reachable from Aruna network
- [ ] Endpoint URL in webhook configuration is correct
- [ ] Authentication token has required permissions
- [ ] Webhook filters match your test data
- [ ] gRPC channel is properly configured
- [ ] S3 credentials are valid
- [ ] Callbacks are being sent
- [ ] Error logs are captured

### Common Issues

**Issue: Webhook never triggers**
- Solution: Check filter configuration, ensure label/name matches
- Verify webhook is registered: 
  - `GET /v2/hooks/owner/{userId}`: List all hooks created by a user
  - `GET /v2/hooks/projects/{projectId}`: List all hooks assigned to a project

**Issue: Download fails**
- Solution: Presigned URL might be expired
- Check network connectivity from service to S3

**Issue: Upload fails**
- Solution: Verify S3 credentials are correct
- Check bucket permissions

**Issue: Callback fails**
- Solution: Verify API token permissions
- Check gRPC channel configuration
- Ensure the Authorization header is set

---

## Deployment

### Docker

```dockerfile
FROM rust:1.70-slim as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/webhook-service /usr/local/bin/

ENV SERVER_HOST=0.0.0.0
ENV SERVICE_PORT=5000

EXPOSE 5000

CMD ["webhook-service"]
```

### Environment Variables

```env
# Required
ARUNA_SERVER_ADDRESS=https://aruna-api.example.com
SERVICE_PORT=5000

# Optional
SERVER_HOST=0.0.0.0
LOG_LEVEL=info
TEMP_DIR=/tmp/webhook-service
PROCESSING_TIMEOUT=300
```

---

## Summary

You now have a complete guide to building webhook services for Aruna. Key takeaways:

1. [x] Implement HTTP endpoint for receiving hooks
2. [x] Set up the gRPC client for callbacks
3. [x] Download files using presigned URLs
4. [x] Process data according to your logic
5. [x] Upload results to S3-compatible storage
6. [x] Create new objects with proper metadata
7. [x] **Always** send status callbacks
8. [x] Handle errors gracefully
9. [x] Test thoroughly before production
10. [x] Deploy with proper monitoring

For the complete working example, see the [ABCD2BioSchema repository](https://github.com/arunaengine/ABCD2BioSchema).

---

## Additional Resources

- [Aruna Documentation](https://docs.aruna-engine.org/)
- [Aruna gRPC API Reference](https://api.aruna-engine.org/)
- [Example Webhook Services: ABCD2BioSchema](https://github.com/arunaengine/ABCD2BioSchema)
- [Example Webhook Services: FASTA-Validator](https://github.com/arunaengine/validator_demo)
