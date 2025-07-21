use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: String,
    pub transformation_id: String,
    pub version_id: String,
    pub input_file_url: String,
    pub input_file_zipped: String,
    pub query: String,
    pub input_file: String,
    pub status: String,
    pub start_time: DateTime<FixedOffset>,
    pub result_file: String,
    pub finish_time: Option<DateTime<FixedOffset>>,
    pub combined_download: String,
    pub job_expiration_date: DateTime<FixedOffset>,
}