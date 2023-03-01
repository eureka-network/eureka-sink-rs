use diesel::{sql_query, RunQueryDsl};
use substreams_sink::Cursor;

use crate::{cursor::CursorLoader, db_loader::Loader, error::DBError};

pub trait FlushLoader {
    fn flush(&mut self, output_module_hash: String, cursor: Cursor) -> Result<(), DBError>;
}

#[allow(dead_code)]
impl FlushLoader for Loader {
    fn flush(&mut self, output_module_hash: String, cursor: Cursor) -> Result<(), DBError> {
        let entries = self.entries().clone();
        let schema = self.get_schema().clone();

        self.connection()
            .build_transaction()
            .read_write()
            .run::<_, diesel::result::Error, _>(|conn| {
                entries.values().for_each(|ops| {
                    for op in ops.values() {
                        let query = op.build_query();
                        // execute the query to the database
                        sql_query(&query)
                            .execute(conn)
                            .map_err(|e| DBError::FailedToExecuteQuery {
                                query: query,
                                error: e.to_string(),
                            })
                            .expect("Failed to execute query");
                    }
                });
                // update the cursors table
                Self::update_cursor_query(&schema, output_module_hash, cursor, conn)
                    .expect("Failed to update cursors table");
                Ok(())
            })
            .map_err(|e| DBError::DieselError(e))?;

        // after flushing, we reset our operation entries
        self.reset()?;

        Ok(())
    }
}

impl Loader {
    fn reset(&mut self) -> Result<(), DBError> {
        self.entries_mut().iter_mut().for_each(|(_, hm)| hm.clear());
        self.reset_entries_count();

        Ok(())
    }
}
