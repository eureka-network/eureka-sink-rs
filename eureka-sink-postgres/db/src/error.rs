use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("DieselError: {0}")]
    DieselError(#[from] diesel::result::Error),
    #[error("ConnectionError: {0}")]
    ConnectionError(#[from] diesel::ConnectionError),
    #[error("Invalid cursor columns")]
    InvalidCursorColumns,
    #[error("Invalid cursor column type")]
    InvalidCursorColumnType,
    #[error("Invalid schema path: {0}")]
    InvalidSchemaPath(#[from] std::io::Error),
    #[error("Invalid field type")]
    InvalidFieldType,
    #[error("Invalid DNS parsing: {0}")]
    InvalidDSNParsing(#[from] dsn::ParseError),
    #[error("FailedToExecuteQuery: query = {query}, error = {error}")]
    FailedToExecuteQuery { query: String, error: String },
}

// impl From<DBError> for diesel::result::Error {
//     fn from(e: DBError) -> Self {
//         match e {
//             DBError::FailedToExecuteQuery { query, error } => {
//                 Self::QueryBuilderError(Box::new(format!("query = {}, error = {}", query, error)))
//             }
//             _ => Self::QueryBuilderError(Box::new("Failed with diesel error")),
//         }
//     }
// }
