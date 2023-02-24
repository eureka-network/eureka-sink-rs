use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("DieselError: {0}")]
    DieselError(#[from] diesel::result::Error),
    #[error("ConnectionError: {0}")]
    ConnectionError(#[from] diesel::ConnectionError),
    #[error("Invalid schema path: {0}")]
    InvalidSchemaPath(#[from] std::io::Error),
    #[error("Invalid field type")]
    InvalidFieldType,
}
