use diesel::{sql_query, PgConnection, QueryableByName, RunQueryDsl};
use substreams_sink::{BlockRef, Cursor};

use crate::{db_loader::DBLoader, error::DBError};

/// Trait to apply changes to the cursors table directly
pub trait CursorLoader {
    /// Given the current state of the DB, gets the correct [`Cursor`] instance
    /// for the given `id = output_module_hash`.
    fn get_cursor(&mut self, output_module_hash: String) -> Result<Cursor, DBError>;
    /// Updates the current state of the `cursors` table, given an `output_module_hash`
    /// value and a [`Cursor`] instance.
    fn update_cursor_query(
        schema: &str,
        module_hash: String,
        cursor: Cursor,
        conn: &mut PgConnection,
    ) -> Result<usize, DBError>;
    /// Writes a new entry to the `cursors` table, given an `output_module_hash` value
    /// and a [`Cursor`] instance.
    fn write_cursor(
        schema: &str,
        module_hash: String,
        cursor: Cursor,
        conn: &mut PgConnection,
    ) -> Result<usize, DBError>;
}

impl CursorLoader for DBLoader {
    fn get_cursor(&mut self, output_module_hash: String) -> Result<Cursor, DBError> {
        #[derive(QueryableByName, Clone)]
        struct CursorRow {
            #[allow(dead_code)]
            #[diesel(sql_type = diesel::sql_types::Text)]
            id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            cursor: String,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            block_num: i64,
            #[diesel(sql_type = diesel::sql_types::Text)]
            block_id: String,
        }

        let query = format!(
            "SELECT id, cursor, block_num, block_id FROM {}.cursors WHERE id = $1",
            self.get_schema(),
        );
        let cursor_rows = sql_query(query)
            .bind::<diesel::sql_types::Text, _>(output_module_hash.clone())
            .load::<CursorRow>(self.connection())
            .map_err(|e| DBError::DieselError(e))?;

        // Selecting on the primary key defines a unique mapping,
        let cursor_row = cursor_rows
            .first()
            .ok_or(DBError::EmptyQuery(output_module_hash))?
            .clone();

        Ok(Cursor {
            cursor: cursor_row.cursor,
            block: BlockRef {
                id: cursor_row.block_id,
                num: u64::try_from(cursor_row.block_num)
                    .map_err(|e| DBError::InvalidColumnDataType(e.to_string()))?,
            },
        })
    }

    fn update_cursor_query(
        schema: &str,
        module_hash: String,
        cursor: Cursor,
        conn: &mut PgConnection,
    ) -> Result<usize, DBError> {
        let query = format!(
            "UPDATE {}.cursors SET cursor = $1, block_num = $2, block_id = $3 WHERE id = $4",
            schema
        );
        sql_query(query)
            .bind::<diesel::sql_types::Text, _>(cursor.cursor)
            .bind::<diesel::sql_types::BigInt, _>(cursor.block.num as i64)
            .bind::<diesel::sql_types::Text, _>(cursor.block.id)
            .bind::<diesel::sql_types::Text, _>(module_hash)
            .execute(conn)
            .map_err(|e| DBError::DieselError(e))
    }

    fn write_cursor(
        schema: &str,
        module_hash: String,
        cursor: Cursor,
        conn: &mut PgConnection,
    ) -> Result<usize, DBError> {
        let query = format!(
            "INSERT INTO {}.cursors (id, cursor, block_num, block_id) VALUES ($1, $2, $3, $4)",
            schema
        );
        sql_query(query)
            .bind::<diesel::sql_types::Text, _>(module_hash)
            .bind::<diesel::sql_types::Text, _>(cursor.cursor)
            .bind::<diesel::sql_types::BigInt, _>(cursor.block.num as i64)
            .bind::<diesel::sql_types::Text, _>(cursor.block.id)
            .execute(conn)
            .map_err(|e| DBError::DieselError(e))
    }
}
