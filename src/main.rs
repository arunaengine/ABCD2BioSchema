use crate::webhook::GfbioWebhook;
use axum::Router;
use axum::routing::{get, post};
use dotenvy::dotenv;
use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig};
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber;

mod job;
mod models;
mod service;
mod webhook;

pub struct Handler {
    pub webhook: GfbioWebhook,
}

pub fn create_router(state: Arc<Handler>) -> Router {
    Router::new()
        .route("/health", get(service::health_check))
        .route("/transform", post(service::upload_and_transform))
        .route("/transform/url", post(service::url_transform))
        .route("/job/{job_id}", get(service::get_job_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Load environment variables from .env file
    dotenv().ok();

    let server_address = dotenvy::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let service_port = dotenvy::var("SERVICE_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .expect("Please select a valid port number of type u16");

    let api_base_url = dotenvy::var("GFBIO_BASE_URL").expect("GFBIO_BASE_URL must be set");
    let temp_dir = dotenvy::var("TEMP_DIR").expect("TEMP_DIR must be set");
    let t_id = dotenvy::var("TRANSFORMATION_ID").unwrap_or("5".to_string());

    let tls_config = ClientTlsConfig::new();
    let aruna_server_address = dotenvy::var("ARUNA_SERVER_ADDRESS").expect("No aruna server set");
    let endpoint = Channel::from_shared(aruna_server_address)
        .unwrap()
        .tls_config(tls_config)
        .unwrap();
    let channel = endpoint.connect().await.unwrap();

    let webhook = GfbioWebhook::with_config(t_id, api_base_url.clone(), temp_dir.clone(), channel);
    let state = Arc::new(Handler { webhook });
    let app = create_router(state);
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", server_address, service_port))
        .await
        .unwrap();

    info!(
        "ABCD2BioSchema Service running on {}:{}",
        server_address, service_port
    );

    println!(
        "ABCD2BioSchema Service running on {}:{}",
        server_address, service_port
    );
    println!("Endpoints:");
    println!("\tPOST\t/transform\t- Upload XML and start transformation");
    println!("\tPOST\t/transform/url\t- Send XML via URL and start transformation");
    println!("\tGET\t/health\t\t- Health check endpoint");

    axum::serve(listener, app).await.unwrap();
}
