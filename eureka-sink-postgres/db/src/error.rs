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
}
