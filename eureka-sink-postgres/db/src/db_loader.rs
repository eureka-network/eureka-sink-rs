use crate::operation::Operation;
use crate::{error::DBError, sql_types::SqlTypeMap};
use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl};
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    path::PathBuf,
};

#[allow(dead_code)]
pub struct Loader {
    connection: PgConnection,
    database: String,
    schema: String,
    entries: HashMap<String, HashMap<String, Operation>>,
    entries_count: u64,
    tables: HashMap<String, HashMap<String, SqlTypeMap>>,
    table_primary_keys: HashMap<String, String>,
}

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
// TODO: rename fields according to query outputs
// TODO: add docs
pub struct RawQueryPrimaryKey {
    #[diesel(sql_type = diesel::sql_types::Text)]
    pk: String,
}

#[derive(QueryableByName, Debug)]
#[allow(dead_code)]
pub struct RawQueryTableNames {
    #[diesel(sql_type = diesel::sql_types::Text)]
    table_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    column_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    column_type: String,
}

#[allow(dead_code)]
impl Loader {
    // TODO: set interface for extracting these values from environment variables
    pub fn new(dsn_string: String, schema_namespace: String) -> Result<Self, DBError> {
        // TODO: do we need to create directory ?
        // create_dir_all(path.parent().unwrap())
        //     .map_err(|_| DBError::FileSystemPathDoesNotExist)?;

        let database = dsn::parse(dsn_string.as_str())
            .map_err(|e| DBError::InvalidDSNParsing(e))?
            .database
            .unwrap_or_default();
        let connection = PgConnection::establish(dsn_string.as_str())
            .map_err(|e| DBError::ConnectionError(e))?;

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

    pub fn reset_entries_count(&mut self) -> u64 {
        let entries_count = self.entries_count;
        self.entries_count = 0;
        entries_count
    }

    pub fn load_tables(&mut self) -> Result<(), DBError> {
        let query_all_tables = format!(
            "
            SELECT
                TABLE_NAME AS table_name
                , COLUMN_NAME AS column_name
                , DATA_TYPE AS column_type
            FROM information_schema.columns
            WHERE table_schema = '{}'
            ORDER BY
                table_name
                , column_name
                , column_type;
        ",
            self.schema
        );
        let all_tables_and_cols = sql_query(query_all_tables)
            .load::<RawQueryTableNames>(self.connection())
            .map_err(|e| DBError::DieselError(e))?;

        let all_tables = all_tables_and_cols
            .iter()
            .map(|q| q.table_name.clone())
            .collect::<HashSet<_>>();

        for table in all_tables {
            let cols = all_tables_and_cols
                .iter()
                .filter(|q| q.table_name == table)
                .map(|q| {
                    (
                        q.column_name.clone(),
                        SqlTypeMap::try_from(q.column_type.as_str()).expect("Invalid field type"),
                    )
                })
                .collect::<HashMap<_, _>>();

            if table.as_str() == "cursors" {
                self.validate_cursor_table(cols.clone())?;
            }

            // update tables mapping
            self.tables.insert(table.clone(), cols);

            let primary_keys = self.get_primary_key_from_table(table.as_str())?;
            let primary_keys = primary_keys
                .iter()
                .map(|pk| pk.clone())
                .collect::<Vec<String>>();
            // TODO: for now we only insert the first primary key column,
            // following the Golang repo. Should we instead be more general ?
            self.table_primary_keys
                .insert(table, primary_keys[0].clone());
        }

        Ok(())
    }

    pub fn validate_cursor_table(
        &mut self,
        columns: HashMap<String, SqlTypeMap>,
    ) -> Result<(), DBError> {
        Self::validate_cursor_table_columns(columns)?;

        // check if primary key has correct name, and thus type
        let pks = self.get_primary_key_from_table("cursors")?;
        let pk = pks[0].as_str();

        if pk != "id" {
            return Err(DBError::InvalidCursorColumnType);
        }

        Ok(())
    }

    fn validate_cursor_table_columns(columns: HashMap<String, SqlTypeMap>) -> Result<(), DBError> {
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
                SqlTypeMap::BigInt | SqlTypeMap::Int8 => {
                    if col_name != "block_num" {
                        return Err(DBError::InvalidCursorColumnType);
                    }
                }
                SqlTypeMap::Text
                | SqlTypeMap::VarChar
                | SqlTypeMap::Char
                | SqlTypeMap::TinyText
                | SqlTypeMap::MediumText
                | SqlTypeMap::LongText => {
                    if col_name == "block_num" {
                        return Err(DBError::InvalidCursorColumnType);
                    }
                }
                _ => return Err(DBError::InvalidCursorColumnType),
            }
        }

        Ok(())
    }

    pub fn get_identifier(&self) -> String {
        format!("{}/{}", self.database, self.schema)
    }

    pub fn get_available_tables_in_schema(&self) -> Vec<String> {
        self.table_primary_keys
            .iter()
            .map(|(s, _)| s.clone())
            .collect::<Vec<_>>()
    }

    pub fn get_schema(&self) -> &String {
        &self.schema
    }

    pub fn get_primary_key_column_name(&self, table_name: &String) -> Option<String> {
        self.table_primary_keys.get(table_name).cloned()
    }

    pub fn get_tables(&self) -> &HashMap<String, HashMap<String, SqlTypeMap>> {
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

    pub(crate) fn increase_entries_count(&mut self) -> u64 {
        let entries_count = self.entries_count;
        self.entries_count += 1;
        entries_count
    }

    pub fn has_table(&self, table: &String) -> bool {
        self.tables.get(table).is_some()
    }

    pub fn set_up_cursor_table(&mut self) -> Result<(), DBError> {
        sql_query(
            "CREATE TABLE IF NOT EXISTS cursors
		(
			id         TEXT NOT NULL CONSTRAINT cursor_pk PRIMARY KEY,
			cursor     TEXT,
			block_num  BIGINT,
			block_id   TEXT
		);
	    ",
        )
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

    pub fn setup_schema(&mut self, setup_file: PathBuf) -> Result<usize, DBError> {
        let setup_query =
            std::fs::read_to_string(setup_file).map_err(|e| DBError::InvalidSchemaPath(e))?;
        let count = sql_query(setup_query)
            .execute(self.connection())
            .map_err(|e| DBError::DieselError(e))?;
        // set a cursors table, as well
        self.set_up_cursor_table()?;
        Ok(count)
    }

    pub fn get_primary_key_from_table(&mut self, table: &str) -> Result<Vec<String>, DBError> {
        let query = format!(
            "
            SELECT a.attname as pk
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{}.{}'::regclass
            AND    i.indisprimary;
        ",
            self.schema, table
        );

        let result = sql_query(query)
            .load::<RawQueryPrimaryKey>(self.connection())
            .map_err(|e| DBError::DieselError(e))?;
        Ok(result.iter().map(|q| q.pk.clone()).collect::<Vec<String>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_validate_cursor_tables() {
        let columns = HashMap::from([
            ("block_num".to_string(), SqlTypeMap::BigInt),
            ("block_id".to_string(), SqlTypeMap::Text),
            ("cursor".to_string(), SqlTypeMap::Text),
            ("id".to_string(), SqlTypeMap::Text),
        ]);
        assert!(Loader::validate_cursor_table_columns(columns).is_ok());
    }
}
