use thiserror::Error;

#[derive(Error, Debug)]
pub enum SubstreamsSinkPostgresError {
    #[error("TonicError: {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),
    #[error("DecodeError: {0}")]
    DecodeError(#[from] prost::DecodeError),
}
