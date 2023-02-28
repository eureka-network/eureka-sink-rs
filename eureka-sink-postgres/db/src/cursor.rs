use diesel::{sql_query, FromSqlRow, QueryableByName, RunQueryDsl};
use std::ops::DerefMut;
use substreams_sink::{BlockRef, Cursor};

use crate::{db_loader::Loader, error::DBError};

pub trait CursorLoader {
    fn get_cursor(&mut self, output_module_hash: String) -> Result<Cursor, DBError>;
    fn update_cursor_query(
        &mut self,
        module_hash: String,
        cursor: Cursor,
    ) -> Result<usize, DBError>;
    fn write_cursor(&mut self, module_hash: String, cursor: Cursor) -> Result<usize, DBError>;
}

impl CursorLoader for Loader {
    fn get_cursor(&mut self, output_module_hash: String) -> Result<Cursor, DBError> {
        #[derive(QueryableByName, Clone)]
        struct CursorRow {
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
            "SELECT id, cursor, block_num, block_id FROM {}.cursors WHERE id = {}",
            self.get_schema(),
            output_module_hash
        );
        let cursor_rows = sql_query(query)
            .load::<CursorRow>(
                self.connection()
                    .expect("Failed to acquire lock")
                    .deref_mut(),
            )
            .map_err(|e| DBError::DieselError(e))?;

        if cursor_rows.is_empty() {
            return Err(DBError::EmptyQuery(output_module_hash));
        }

        // We already checked that the query is not empty, as this point. Moreover,
        // we are selecting on the primary key, which we know defines a unique mapping
        // key -> valuue, so the cursor_rows.len() == 1
        let cursor_row = cursor_rows[0].clone();

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
        &mut self,
        module_hash: String,
        cursor: Cursor,
    ) -> Result<usize, DBError> {
        let query = "UPDATE cursors SET cursor = ?, block_num = ?, block_id = ? WHERE id = ?";
        sql_query(query)
            .bind::<diesel::sql_types::Text, _>(cursor.cursor)
            .bind::<diesel::sql_types::BigInt, _>(cursor.block.num as i64)
            .bind::<diesel::sql_types::Text, _>(cursor.block.id)
            .bind::<diesel::sql_types::Text, _>(module_hash)
            .execute(
                self.connection()
                    .expect("Failed to acquire lock")
                    .deref_mut(),
            )
            .map_err(|e| DBError::DieselError(e))
    }

    fn write_cursor(&mut self, module_hash: String, cursor: Cursor) -> Result<usize, DBError> {
        let query = "INSERT INTO cursors (id, cursor, block_num, block_id) VALUES (?, ?, ?, ?)";
        sql_query(query)
            .bind::<diesel::sql_types::Text, _>(module_hash)
            .bind::<diesel::sql_types::Text, _>(cursor.cursor)
            .bind::<diesel::sql_types::BigInt, _>(cursor.block.num as i64)
            .bind::<diesel::sql_types::Text, _>(cursor.block.id)
            .execute(
                self.connection()
                    .expect("Failed to acquire lock")
                    .deref_mut(),
            )
            .map_err(|e| DBError::DieselError(e))
    }
}
