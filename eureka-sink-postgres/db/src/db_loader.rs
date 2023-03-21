use crate::operation::Operation;
use crate::{error::DBError, sql_types::ColumnType};
use diesel::connection::SimpleConnection;
use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl};
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    path::PathBuf,
};

#[allow(dead_code)]
/// [`DBLoader`] provides an interface to deal with a PostgreSQL database, which is
/// suitable to be used as a sink to Substreams data (https://substreams.streamingfast.io/developers-guide/sink-targets).
/// It provides functionality to deal with generic tables as well as a `cursors` table
/// (https://substreams.streamingfast.io/developers-guide/sink-targets/substreams-sink-postgres#cursors).
pub struct DBLoader {
    /// A PostgresSQL connection to a postgres instance.
    connection: PgConnection,
    /// Database name
    database: String,
    /// Current schema, in which tables exists.
    schema: String,
    /// Mapping from table name to column name to operation.
    entries: HashMap<String, HashMap<String, Operation>>,
    /// number of entries, i.e., total size of `entries` above.
    entries_count: u64,
    /// Table metadata, in the form of a mapping from table name
    /// to column field to its underlying type.
    tables: HashMap<String, HashMap<String, ColumnType>>,
    /// For each table_name we provide an array of its primary key column names.
    table_primary_keys: HashMap<String, String>,
}

#[allow(dead_code)]
impl DBLoader {
    pub fn new(dsn_string: String, schema_namespace: String) -> Result<Self, DBError> {
        let database = dsn::parse(dsn_string.as_str())
            .map_err(|e| DBError::InvalidDSNParsing(e))?
            .database
            .unwrap_or_default();
        let mut connection = PgConnection::establish(dsn_string.as_str())
            .map_err(|e| DBError::ConnectionError(e))?;

        // if schema does not exist in the DB, create it
        let query = format!("CREATE SCHEMA IF NOT EXISTS {}", schema_namespace);
        sql_query(query)
            .execute(&mut connection)
            .map_err(|e| DBError::DieselError(e))?;

        Ok(Self {
            connection: connection,
            database,
            schema: schema_namespace,
            entries: HashMap::new(),
            entries_count: 0,
            tables: HashMap::new(),
            table_primary_keys: HashMap::new(),
        })
    }

    /// Resets all entries count to 0.
    pub fn reset_entries_count(&mut self) -> u64 {
        let entries_count = self.entries_count;
        self.entries_count = 0;
        entries_count
    }

    /// Loads all necessary tables that exist for the current schema and DB.
    pub fn load_tables(&mut self) -> Result<(), DBError> {
        #[derive(QueryableByName, Debug)]
        pub struct TableMetadata {
            #[diesel(sql_type = diesel::sql_types::Text)]
            table_name: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            column_name: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            column_type: String,
        }

        let query = " SELECT
                TABLE_NAME AS table_name
                , COLUMN_NAME AS column_name
                , DATA_TYPE AS column_type
            FROM information_schema.columns
            WHERE table_schema = $1
            ORDER BY
                table_name
                , column_name
                , column_type;
        ";

        let all_tables_and_cols = sql_query(query)
            .bind::<diesel::sql_types::Text, _>(self.schema.clone())
            .load::<TableMetadata>(self.connection())
            .map_err(|e| DBError::DieselError(e))?;

        let all_tables = all_tables_and_cols
            .iter()
            .map(|q| q.table_name.clone())
            .collect::<HashSet<_>>();

        for table in all_tables {
            let cols = all_tables_and_cols
                .iter()
                .filter_map(|q| {
                    if q.table_name == table {
                        Some((
                            q.column_name.clone(),
                            ColumnType::try_from(q.column_type.as_str())
                                .expect("Invalid field type"),
                        ))
                    } else {
                        None
                    }
                })
                .collect::<HashMap<_, _>>();

            if table.as_str() == "cursors" {
                self.validate_cursor_table(cols.clone())?;
            }

            // update tables mapping
            self.tables.insert(table.clone(), cols);

            let primary_key = self.get_primary_key_from_table(table.as_str())?;

            // TODO: for now we only insert the first primary key column,
            // following the Golang repo. Should we instead be more general ?

            self.table_primary_keys.insert(table, primary_key);
        }

        Ok(())
    }

    /// Validates the `cursors` table. This is important, as this table
    /// follows a very speficic format `(block_num, block_id, cursor, id)`.
    pub fn validate_cursor_table(
        &mut self,
        columns: HashMap<String, ColumnType>,
    ) -> Result<(), DBError> {
        Self::validate_cursor_table_columns(columns)?;

        // check if primary key has correct name, and thus type
        let pk = self.get_primary_key_from_table("cursors")?;

        if pk.as_str() != "id" {
            return Err(DBError::InvalidCursorColumnType);
        }

        Ok(())
    }

    /// Auxiliary function used in [`validate_cursor_table`].
    fn validate_cursor_table_columns(columns: HashMap<String, ColumnType>) -> Result<(), DBError> {
        if columns.len() != 4 {
            return Err(DBError::InvalidCursorColumns);
        }

        let available_columns = vec!["block_num", "block_id", "cursor", "id"];
        available_columns
            .iter()
            .map(|c| {
                columns
                    .get(&c.to_string())
                    .ok_or(DBError::InvalidCursorColumns)
            })
            .collect::<Result<Vec<_>, DBError>>()?;

        for (col_name, col) in columns {
            if !available_columns.contains(&col_name.as_str()) {
                return Err(DBError::InvalidCursorColumns);
            }

            match col {
                ColumnType::BigInt => {
                    if col_name != "block_num" {
                        return Err(DBError::InvalidCursorColumnType);
                    }
                }
                ColumnType::Text => {
                    if col_name == "block_num" {
                        return Err(DBError::InvalidCursorColumnType);
                    }
                }
                _ => return Err(DBError::InvalidCursorColumnType),
            }
        }

        Ok(())
    }

    /// Gets database schema identifier, in the form db.schema.
    pub fn get_identifier(&self) -> String {
        format!("{}/{}", self.database, self.schema)
    }

    /// Get all table names in our schema.
    pub fn get_available_tables_in_schema(&self) -> Vec<String> {
        self.table_primary_keys
            .iter()
            .map(|(s, _)| s.clone())
            .collect::<Vec<_>>()
    }

    pub fn get_schema(&self) -> &String {
        &self.schema
    }

    pub fn get_primary_key_column_name(&self, table_name: &str) -> Option<String> {
        self.table_primary_keys.get(table_name).cloned()
    }

    pub fn get_tables(&self) -> &HashMap<String, HashMap<String, ColumnType>> {
        &self.tables
    }

    pub fn get_entries_count(&self) -> u64 {
        self.entries_count
    }

    pub(crate) fn get_entries_mut(&mut self) -> &mut HashMap<String, HashMap<String, Operation>> {
        &mut self.entries
    }

    pub fn get_entries(&self) -> &HashMap<String, HashMap<String, Operation>> {
        &self.entries
    }

    /// It increases by 1 the `entries_count` value. To be used, whenever
    /// a new entry is inserted in [`tables`].
    pub(crate) fn increase_entries_count(&mut self) -> u64 {
        let entries_count = self.entries_count;
        self.entries_count += 1;
        entries_count
    }

    /// Checks if `table` exists in the current db and schema state.
    pub fn has_table(&self, table: &str) -> bool {
        self.tables.get(table).is_some()
    }

    pub fn set_up_cursor_table(&mut self) -> Result<(), DBError> {
        sql_query(format!(
            "CREATE TABLE IF NOT EXISTS {}.cursors
		(
			id         TEXT NOT NULL CONSTRAINT cursor_pk PRIMARY KEY,
			cursor     TEXT,
			block_num  BIGINT,
			block_id   TEXT
		);
	    ",
            self.get_schema().clone()
        ))
        .execute(self.connection())
        .map_err(|e| DBError::DieselError(e))?;

        Ok(())
    }

    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    pub(crate) fn entries(&self) -> &HashMap<String, HashMap<String, Operation>> {
        &self.entries
    }

    pub(crate) fn entries_mut(&mut self) -> &mut HashMap<String, HashMap<String, Operation>> {
        &mut self.entries
    }

    /// Given a a file path, assumed to be of .sql extension, it executes all queries
    /// in that file. The goal is to create, if necessary, all necessary tables in the
    /// schema. It also sets up a [`cursors`] table.
    pub fn setup_schema(&mut self, setup_file: PathBuf) -> Result<(), DBError> {
        let setup_query =
            std::fs::read_to_string(setup_file).map_err(|e| DBError::InvalidSchemaPath(e))?;
        self.connection()
            .batch_execute(&setup_query)
            .map_err(|e| DBError::DieselError(e))?;
        // set a cursors table, as well
        self.set_up_cursor_table()?;
        Ok(())
    }

    /// Given a table name, it outputs its primary key column name
    fn get_primary_key_from_table(&mut self, table: &str) -> Result<String, DBError> {
        // auxiliary type to be used as the output of executing query
        #[derive(QueryableByName, Debug)]
        pub struct PrimaryKey {
            #[diesel(sql_type = diesel::sql_types::Text)]
            pk: String,
        }

        let query = format!(
            "
            SELECT a.attname as pk
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{}.{}'::regclass
            AND    i.indisprimary;
            ",
            self.schema.clone(),
            table
        );

        let primary_keys = sql_query(query)
            .load::<PrimaryKey>(self.connection())
            .map_err(|e| DBError::DieselError(e))?;

        // For now we assume our tables only have one primary key column
        let primary_key = primary_keys.first().ok_or(DBError::EmptyQuery(format!(
            "Unable to query the primary key for table {}",
            table
        )))?;

        Ok(primary_key.pk.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_validate_cursor_tables() {
        let columns = HashMap::from([
            ("block_num".to_string(), ColumnType::BigInt),
            ("block_id".to_string(), ColumnType::Text),
            ("cursor".to_string(), ColumnType::Text),
            ("id".to_string(), ColumnType::Text),
        ]);
        assert!(DBLoader::validate_cursor_table_columns(columns).is_ok());
    }
}
