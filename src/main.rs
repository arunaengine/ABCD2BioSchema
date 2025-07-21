use axum::{Router};
use axum::routing::{get, post};
use tower_http::cors::CorsLayer;
use dotenvy::dotenv;
use tracing::{info};
use tracing_subscriber;

mod job;
mod service;
mod webhook;
mod models;

pub fn create_router() -> Router {
    Router::new()
        .route("/health", get(service::health_check))
        .route("/transform", post(service::upload_and_transform))
        .route("/transform/url", post(service::url_transform))
        .route("/job/{job_id}", get(service::get_job_status))
        .layer(CorsLayer::permissive())
}

#[tokio::main]
async fn main() {

    tracing_subscriber::fmt::init();

    // Load environment variables from .env file
    dotenv().ok();

    let server_address = dotenvy::var("SERVER_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());
    let service_port = dotenvy::var("SERVICE_PORT").unwrap_or_else(|_| "3000".to_string()).parse::<u16>().expect("Please select a valid port number of type u16");

    let app = create_router();

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", server_address, service_port))
        .await
        .unwrap();

    info!("ABCD2BioSchema Service running on {}:{}", server_address, service_port);

    println!("ABCD2BioSchema Service running on {}:{}", server_address, service_port);
    println!("Endpoints:");
    println!("\tPOST\t/transform\t- Upload XML and start transformation");
    println!("\tPOST\t/transform/url\t- Send XML via URL and start transformation");
    println!("\tGET\t/health\t\t- Health check endpoint");

    axum::serve(listener, app).await.unwrap();
}