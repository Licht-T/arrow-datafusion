use crate::error::Result;

use std::str::FromStr;
use crate::DataFusionError;

#[derive(Debug, PartialEq, Eq, clap::ArgEnum, Clone)]
pub enum ObjectStoreScheme {
    S3,
    S3SELECT,
    GCS,
}

impl FromStr for ObjectStoreScheme {
    type Err = DataFusionError;

    fn from_str(input: &str) -> Result<Self> {
        match input {
            "s3" => Ok(ObjectStoreScheme::S3),
            "s3select" => Ok(ObjectStoreScheme::S3SELECT),
            "gcs" => Ok(ObjectStoreScheme::GCS),
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported object store scheme {}",
                input
            ))),
        }
    }
}
