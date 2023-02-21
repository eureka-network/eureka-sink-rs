// use diesel::ConnectionError;
// use thiserror::Error;

// #[derive(Error, Debug)]
// pub enum SubstreamsSinkPostgresError {
//     #[error("TonicError: {0}")]
//     TonicError(#[from] tonic::transport::Error),
//     #[error("IoError: {0}")]
//     IoError(#[from] std::io::Error),
//     #[error("DecodeError: {0}")]
//     DecodeError(#[from] prost::DecodeError),
//     #[error("PostgresError")]
//     PostgresError,
//     #[error("File system path does not exist")]
//     FileSystemPathDoesNotExist,
//     #[error("Connection Error: {0}")]
//     ConnectionError(#[from] ConnectionError),
//     #[error("Diesel error during operation {operation}: {source}")]
//     DieselError {
//         source: diesel::result::Error,
//         operation: String,
//     },
// }
