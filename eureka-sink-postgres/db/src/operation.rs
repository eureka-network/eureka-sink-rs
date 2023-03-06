use crate::sql_types::ColumnValue;
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
/// Records the type of each of the available operations (insert, update, delete).
pub enum OperationType {
    Insert,
    Update,
    Delete,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
/// [`Operation`] provides interface to apply changes to the DB, via a [`DBLoader`]
/// instance.
pub struct Operation {
    /// The schema name.
    schema_name: String,
    /// The table name.
    table_name: String,
    /// The primary key column name, for the current table.
    primary_key_column_name: String,
    /// Checks which db operation is, either insert, update or delete.
    op_type: OperationType,
    /// Primary key value, with type already parsed in.
    primary_key: ColumnValue,
    /// The data to be applied on the operation. Consists of a mapping from
    /// column name to its value, with type parsed in.
    data: HashMap<String, ColumnValue>, // mapping data row from columns -> field
}

#[allow(dead_code)]
impl Operation {
    pub fn new(
        schema_name: String,
        table_name: String,
        primary_key_column_name: String,
        op_type: OperationType,
        primary_key: ColumnValue,
        data: HashMap<String, ColumnValue>,
    ) -> Self {
        Self {
            schema_name,
            table_name,
            primary_key_column_name,
            op_type,
            primary_key,
            data,
        }
    }

    /// Builds a query to be executed by a [`DBLoader`] instance, which depends
    /// on the operation type and the provided data, as well as the primary key value.
    pub fn build_query(&self) -> String {
        let query = match self.op_type {
            OperationType::Delete => {
                format!(
                    "DELETE FROM {}.{} WHERE {} = {}",
                    self.schema_name,
                    self.table_name,
                    self.primary_key_column_name,
                    self.primary_key.to_string()
                )
            }
            OperationType::Insert => {
                let mut keys = "".to_string();
                let mut values = "".to_string();

                self.data.iter().for_each(|(k, v)| {
                    keys.push_str(format!(",{}", k).as_str());
                    values.push_str(format!(",{}", v.to_string()).as_str());
                });
                // remove extra initial ','
                keys.remove(0); // ,col1,col2,col3,col4 -> col1,col2,col3,col4
                values.remove(0); // ,val1,val2,val3,val4 -> val1,val2,val3,val4

                format!(
                    "INSERT INTO {}.{} ({}) VALUES ({})",
                    self.schema_name, self.table_name, keys, values
                )
            }
            OperationType::Update => {
                let mut updates = "".to_string();

                self.data.iter().for_each(|(k, v)| {
                    updates.push_str(format!(",{}={}", k, v.to_string()).as_str())
                });
                // remove extra initial ','
                updates.remove(0); //,col1=val1,col2=val2,col3=val3 -> col1=val1,col2=val2,col3=val3
                format!(
                    "UPDATE {}.{} SET {} WHERE {}={}",
                    self.schema_name,
                    self.table_name,
                    updates,
                    self.primary_key_column_name,
                    self.primary_key.to_string()
                )
            }
        };

        query
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn primary_key_column_name(&self) -> &str {
        &self.primary_key_column_name
    }

    pub fn primary_key(&self) -> &ColumnValue {
        &self.primary_key
    }

    pub fn op_type(&self) -> &OperationType {
        &self.op_type
    }

    pub fn data(&self) -> &HashMap<String, ColumnValue> {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::sql_types::{Binary, Date, Integer, Sql, Text};

    use super::*;

    #[test]
    fn it_works_build_delete_query() {
        let data = HashMap::from([
            (
                "col1".to_string(),
                ColumnValue::Integer(Integer::set_inner(10)),
            ),
            (
                "col2".to_string(),
                ColumnValue::Date(Date::set_inner(
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                )),
            ),
            (
                "col3".to_string(),
                ColumnValue::Binary(Binary::set_inner(vec![0u8, 1, 2])),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Delete,
            ColumnValue::Text(Text::set_inner("field_to_delete".to_string())),
            data,
        );

        let query = operation.build_query();

        assert_eq!(
            query,
            "DELETE FROM my_scheme.my_table WHERE my_primary_key_column_name = 'field_to_delete'"
        );
    }

    #[test]
    fn it_works_build_insert_into_query() {
        let data = HashMap::from([
            (
                "col1".to_string(),
                ColumnValue::Integer(Integer::set_inner(10)),
            ),
            (
                "col2".to_string(),
                ColumnValue::Date(Date::set_inner(
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                )),
            ),
            (
                "col3".to_string(),
                ColumnValue::Binary(Binary::set_inner(vec![0u8, 1, 2])),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Insert,
            ColumnValue::Text(Text::set_inner("field_to_delete".to_string())),
            data,
        );

        let query = operation.build_query();

        // hash maps don't order store their (k, v) pairs
        // for this reason, it is possible to have six, and only six,
        // different queries
        let possible_queries = [
            "INSERT INTO my_scheme.my_table (col1,col2,col3) VALUES (10,'2023-03-01',[0, 1, 2])",
            "INSERT INTO my_scheme.my_table (col2,col1,col3) VALUES ('2023-03-01',10,[0, 1, 2])",
            "INSERT INTO my_scheme.my_table (col2,col3,col1) VALUES ('2023-03-01',[0, 1, 2],10)",
            "INSERT INTO my_scheme.my_table (col3,col1,col2) VALUES ([0, 1, 2],10,'2023-03-01')",
            "INSERT INTO my_scheme.my_table (col3,col2,col1) VALUES ([0, 1, 2],'2023-03-01',10)",
            "INSERT INTO my_scheme.my_table (col1,col3,col2) VALUES (10,[0, 1, 2],'2023-03-01')",
        ];

        assert!(possible_queries.contains(&query.as_str()));
    }

    #[test]
    fn it_works_build_update_query() {
        let data = HashMap::from([
            (
                "col1".to_string(),
                ColumnValue::Integer(Integer::set_inner(10_i32)),
            ),
            (
                "col2".to_string(),
                ColumnValue::Date(Date::set_inner(
                    NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                )),
            ),
            (
                "col3".to_string(),
                ColumnValue::Binary(Binary::set_inner(vec![0u8, 1, 2])),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Update,
            ColumnValue::Text(Text::set_inner("field_to_delete".to_string())),
            data,
        );

        let query = operation.build_query();

        // hash maps don't order store their (k, v) pairs
        // for this reason, it is possible to have six, and only six,
        // different queries
        let possible_queries = [
            "UPDATE my_scheme.my_table SET col1=10,col2='2023-03-01',col3=[0, 1, 2] WHERE my_primary_key_column_name='field_to_delete'",
            "UPDATE my_scheme.my_table SET col3=[0, 1, 2],col2='2023-03-01',col1=10 WHERE my_primary_key_column_name='field_to_delete'",
            "UPDATE my_scheme.my_table SET col3=[0, 1, 2],col1=10,col2='2023-03-01' WHERE my_primary_key_column_name='field_to_delete'",
            "UPDATE my_scheme.my_table SET col2='2023-03-01',col1=10,col3=[0, 1, 2] WHERE my_primary_key_column_name='field_to_delete'",
            "UPDATE my_scheme.my_table SET col2='2023-03-01',col3=[0, 1, 2],col1=10 WHERE my_primary_key_column_name='field_to_delete'",
            "UPDATE my_scheme.my_table SET col1=10,col3=[0, 1, 2],col2='2023-03-01' WHERE my_primary_key_column_name='field_to_delete'"
        ];

        assert!(possible_queries.contains(&query.as_str()));
    }
}
