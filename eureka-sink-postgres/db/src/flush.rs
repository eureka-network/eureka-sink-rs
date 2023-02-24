use diesel::{sql_query, RunQueryDsl};

use crate::{db_loader::Loader, error::DBError};

#[allow(dead_code)]
impl Loader {
    fn flush(&mut self) -> Result<(), DBError> {
        self.connection()
            .expect("Failed to acquire lock")
            .build_transaction()
            .read_write()
            .run::<_, diesel::result::Error, _>(|conn| {
                self.entries().values().for_each(|ops| {
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

                    // TODO: add cursor logic
                });
                Ok(())
            })
            .map_err(|e| DBError::DieselError(e))?;

        self.reset().expect("Unable to reset loader");

        Ok(())
    }

    fn reset(&mut self) -> Result<(), DBError> {
        self.entries_mut().iter_mut().for_each(|(_, hm)| hm.clear());
        self.reset_entries_count();

        Ok(())
    }
}
