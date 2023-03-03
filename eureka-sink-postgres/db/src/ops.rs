use std::collections::HashMap;

use crate::{
    db_loader::DBLoader,
    error::DBError,
    operation::{Operation, OperationType},
    sql_types::ColumnValue,
};

/// [`DBLoaderOperations`] introduces an interface to apply operations/changes
/// to a DB, via a [`DBLoader`] instance.
pub trait DBLoaderOperations {
    /// Inserts a new [`Delete`] operation in the [`DBLoader`]
    fn delete(
        &mut self,
        table_name: String,
        primary_key: String,
        data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError>;
    /// Inserts a new [`Insert`] operation in the [`DBLoader`]
    fn insert(
        &mut self,
        table_name: String,
        primary_key: String,
        data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError>;
    /// Inserts a new [`Update`] operation in the [`DBLoader`]
    fn update(
        &mut self,
        table_name: String,
        primary_key: String,
        data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError>;
}

#[allow(dead_code)]
impl DBLoaderOperations for DBLoader {
    fn delete(
        &mut self,
        _table_name: String,
        _primary_key: String,
        _data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError> {
        unimplemented!("To be implemented!")
    }

    fn insert(
        &mut self,
        table_name: String,
        primary_key: String,
        data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError> {
        // get primary key correct field type
        let primary_key_colname = self
            .get_primary_key_column_name(&table_name)
            .expect(format!("Primary key not found for current table {}", &table_name).as_str());
        let primary_key_val =
            self.get_type(&table_name, &primary_key_colname, primary_key.clone())?;
        // get data correct field type
        let data = data
            .iter()
            .map(|(colname, val)| (colname.clone(), val.clone()))
            .collect::<HashMap<String, ColumnValue>>();
        // retrieve insert operation
        let insert_op = self.new_insert_operation(table_name.clone(), primary_key_val, data);

        let entries = self.get_entries_mut();
        // if the current table_name does not exist in our entries hashmap, we simply
        // add to it, with empty operations
        let ops = entries.entry(table_name.clone()).or_default();

        // verify that the primary key hasn't been scheduled for a previous operation
        // no update is performed on insert, append only
        if ops.get(&primary_key).is_some() {
            return Err(DBError::PrimaryKeyAlreadyScheduleForOperation {
                table_name,
                primary_key,
            });
        }

        // we are guaranteed to insert the op in the hashmap, as we verified above that this didn't
        // contain any already scheduled op for the given primary key and table
        ops.entry(primary_key).or_insert(insert_op);
        self.increase_entries_count();

        Ok(())
    }

    fn update(
        &mut self,
        _table_name: String,
        _primary_key: String,
        _data: HashMap<String, ColumnValue>,
    ) -> Result<(), DBError> {
        unimplemented!("To be implemented!")
    }
}

impl DBLoader {
    /// Gets the the value of a column, with type already parsed in.
    fn get_type(
        &self,
        table_name: &String,
        column_name: &String,
        value: String,
    ) -> Result<ColumnValue, DBError> {
        let table_cols = self
            .get_tables()
            .get(table_name)
            .ok_or(DBError::TableNotFound(table_name.clone()))?;

        let col_type = table_cols
            .get(column_name)
            .ok_or(DBError::ColumnNotFound(column_name.clone()))?;

        ColumnValue::parse_type(col_type.clone(), value)
    }

    /// Given a table name, a primary key and provided data, it creates a
    /// new operation, of type `Insert`.
    fn new_insert_operation(
        &self,
        table_name: String,
        primary_key: ColumnValue,
        data: HashMap<String, ColumnValue>,
    ) -> Operation {
        Operation::new(
            self.get_schema().clone(),
            table_name.clone(),
            self.get_primary_key_column_name(&table_name)
                .expect(format!("Primary key column not valid for table: {}", table_name).as_ref()),
            OperationType::Insert,
            primary_key,
            data,
        )
    }
}
