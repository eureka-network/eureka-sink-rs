use std::collections::HashMap;

use crate::{db_loader::Loader, error::DBError, sql_types::SqlType};

impl Loader {
    pub fn _insert(
        &mut self,
        _table_name: String,
        _primary_key: String,
        _data: HashMap<String, SqlType>,
    ) -> Result<(), DBError> {
        Ok(())
    }

    pub fn _update(
        &mut self,
        _table_name: String,
        _primary_key: String,
        _data: HashMap<String, SqlType>,
    ) -> Result<(), DBError> {
        Ok(())
    }

    pub fn _delete(
        &mut self,
        _table_name: String,
        _primary_key: String,
        _data: HashMap<String, SqlType>,
    ) -> Result<(), DBError> {
        Ok(())
    }
}
