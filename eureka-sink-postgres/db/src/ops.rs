use std::collections::HashMap;

use crate::{
    db_loader::Loader,
    error::DBError,
    operation::{Operation, OperationType},
    sql_types::{SqlType, SqlTypeMap},
};

#[allow(dead_code)]
impl Loader {
    pub fn insert(
        &mut self,
        table_name: String,
        primary_key: String,
        data: HashMap<String, String>,
    ) -> Result<(), DBError> {
        // get primary key correct field type
        let primary_key_colname = self
            .get_primary_key_column_name(&table_name)
            .expect(format!("Primary key not found for current table {}", &table_name).as_str());
        let primary_key_val = self.get_type(&table_name, &primary_key_colname, &primary_key)?;
        // get data correct field type
        let data = data
            .iter()
            .map(|(colname, val)| {
                (
                    colname.clone(),
                    self.get_type(&table_name, &colname, &val)
                        .expect("Invalid parsing of type"),
                )
            })
            .collect::<HashMap<String, SqlType>>();
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
}

impl Loader {
    fn get_type(
        &self,
        table_name: &String,
        column_name: &String,
        value: &String,
    ) -> Result<SqlType, DBError> {
        let table_cols = self
            .get_tables()
            .get(table_name)
            .ok_or(DBError::TableNotFound(table_name.clone()))?;

        let col_type = table_cols
            .get(column_name)
            .ok_or(DBError::ColumnNotFound(column_name.clone()))?;

        Ok(match col_type {
            SqlTypeMap::Bool => SqlType::Bool(crate::sql_types::Bool {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::SmallInt => SqlType::SmallInt(crate::sql_types::SmallInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int2 => SqlType::Int2(crate::sql_types::Int2 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::BigInt => SqlType::BigInt(crate::sql_types::BigInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Integer => SqlType::Integer(crate::sql_types::Integer {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int4 => SqlType::Int4(crate::sql_types::Int4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int8 => SqlType::Int8(crate::sql_types::Int8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float => SqlType::Float(crate::sql_types::Float {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float4 => SqlType::Float4(crate::sql_types::Float4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float8 => SqlType::Float8(crate::sql_types::Float8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Numeric => SqlType::Numeric(crate::sql_types::Numeric {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
        })
    }

    fn new_insert_operation(
        &self,
        table_name: String,
        primary_key: SqlType,
        data: HashMap<String, SqlType>,
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
