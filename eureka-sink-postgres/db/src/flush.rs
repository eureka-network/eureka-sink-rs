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
        if let Err(e) = self
            .connection()
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
                Ok(())
            })
            .map_err(|e| DBError::DieselError(e))
        {
            // if the transaction failed, we error
            return Err(e);
        } else {
            // otherwise, it is safe to update the cursors table
            self.update_cursor_query(output_module_hash, cursor)?;
        }

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
