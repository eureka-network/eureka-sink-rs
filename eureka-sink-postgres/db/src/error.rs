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
    #[error("Table {0} not found")]
    TableNotFound(String),
    #[error(
        "Primary key {primary_key} already scheduled for previous operation, on table {table_name}"
    )]
    PrimaryKeyAlreadyScheduleForOperation {
        table_name: String,
        primary_key: String,
    },
    #[error("Column {0} not found")]
    ColumnNotFound(String),
    #[error("Failed to parse value {0}")]
    FailedParseString(String),
    #[error("Failed to execute query: {query} with error: {error}")]
    FailedToExecuteQuery { query: String, error: String },
    #[error("Empty query for param: {0}")]
    EmptyQuery(String),
    #[error("Invalid column data type: {0}")]
    InvalidColumnDataType(String),
    #[error("Unique violation error")]
    UniqueViolation,
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
