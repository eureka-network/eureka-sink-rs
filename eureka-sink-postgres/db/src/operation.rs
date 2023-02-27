use crate::sql_types::SqlType;
use std::collections::HashMap;

#[allow(dead_code)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
}

#[allow(dead_code)]
pub struct Operation {
    schema_name: String,
    table_name: String,
    primary_key_column_name: String,
    op_type: OperationType,
    primary_key: SqlType,
    data: HashMap<String, SqlType>, // mapping data row from columns -> field
}

#[allow(dead_code)]
impl Operation {
    pub fn new(
        schema_name: String,
        table_name: String,
        primary_key_column_name: String,
        op_type: OperationType,
        primary_key: SqlType,
        data: HashMap<String, SqlType>,
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

    // TODO: need to parse sql query in accordance with the data type
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

                // TODO: write this in a more idiomatic way
                self.data.iter().for_each(|(k, v)| {
                    keys.push_str(format!(",{}", k).as_str());
                    values.push_str(format!(",{}", v.to_string()).as_str());
                });
                // remove extra initial ','
                keys.remove(0); // ,col1,col2,col3,col4 -> col1,col2,col3,col4
                values.remove(0); //

                format!(
                    "INSERT INTO {}.{} ({}) VALUES ({})",
                    self.schema_name, self.table_name, keys, values
                )
            }
            OperationType::Update => {
                let mut updates = "".to_string();
                // TODO; write this more idiomatically
                self.data.iter().for_each(|(k, v)| {
                    updates.push_str(format!(",{}={}", k, v.to_string()).as_str())
                });
                // remove extra initial ','
                updates.remove(0);
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
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::sql_types::{Binary, Date, Integer, Text};

    use super::*;

    #[test]
    fn it_works_build_delete_query() {
        let data = HashMap::from([
            (
                "col1".to_string(),
                SqlType::Integer(Integer { inner: 10_u32 }),
            ),
            (
                "col2".to_string(),
                SqlType::Date(Date {
                    inner: NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                }),
            ),
            (
                "col3".to_string(),
                SqlType::Binary(Binary {
                    inner: vec![0u8, 1, 2],
                }),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Delete,
            SqlType::Text(Text {
                inner: "field_to_delete".to_string(),
            }),
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
                SqlType::Integer(Integer { inner: 10_u32 }),
            ),
            (
                "col2".to_string(),
                SqlType::Date(Date {
                    inner: NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                }),
            ),
            (
                "col3".to_string(),
                SqlType::Binary(Binary {
                    inner: vec![0u8, 1, 2],
                }),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Insert,
            SqlType::Text(Text {
                inner: "field_to_delete".to_string(),
            }),
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
                SqlType::Integer(Integer { inner: 10_u32 }),
            ),
            (
                "col2".to_string(),
                SqlType::Date(Date {
                    inner: NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
                }),
            ),
            (
                "col3".to_string(),
                SqlType::Binary(Binary {
                    inner: vec![0u8, 1, 2],
                }),
            ),
        ]);

        let operation = Operation::new(
            "my_scheme".to_string(),
            "my_table".to_string(),
            "my_primary_key_column_name".to_string(),
            OperationType::Update,
            SqlType::Text(Text {
                inner: "field_to_delete".to_string(),
            }),
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
