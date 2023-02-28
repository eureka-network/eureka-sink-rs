use std::{collections::HashMap, path::PathBuf};

use eureka_sink_postgres::{
    db_loader::Loader,
    operation::{Operation, OperationType},
    sql_types::{SqlType, SqlTypeMap, Text},
};

const DATABASE_URL: &str =
    "postgres://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable";

#[test]
fn dbloader_test() {
    let schema_namespace = String::from("public");
    let mut loader = Loader::new(String::from(DATABASE_URL), schema_namespace).unwrap();

    let schema_file = PathBuf::try_from("../../tests/dbloader/sql/schema.sql").unwrap();
    let count = loader.setup_schema(schema_file).unwrap();

    assert_eq!(count, 0);
    assert_eq!(loader.get_schema(), "public")
}

#[test]
fn it_works_load_tables() {
    let database_url = DATABASE_URL.to_string();
    let schema_namespace = String::from("public");
    let mut loader = Loader::new(database_url, schema_namespace).unwrap();

    let schema_file = PathBuf::try_from("../../tests/dbloader/sql/schema.sql").unwrap();
    loader.setup_schema(schema_file).unwrap();

    assert!(loader.load_tables().is_ok());

    // assert tables are correctly specified
    let tables = loader.get_available_tables_in_schema();
    assert_eq!(tables.len(), 2);
    assert!(
        tables.contains(&String::from("block_meta")) && tables.contains(&String::from("cursors"))
    );

    // assert that column types are correctly specified
    let columns_per_table = loader.get_tables();
    let block_meta_columns = columns_per_table.get(&"block_meta".to_string()).unwrap();
    for k in block_meta_columns.keys() {
        match k.as_str() {
            "parent_hash" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "timestamp" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "at" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "hash" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "number" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Integer),
            "id" => assert_eq!(block_meta_columns.get(k).unwrap(), &SqlTypeMap::Text),
            _ => panic!("Invalid field name for table block_meta"),
        }
    }

    let cursors_columns = columns_per_table.get("cursors").unwrap();
    for k in cursors_columns.keys() {
        match k.as_str() {
            "id" => assert_eq!(cursors_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "cursor" => assert_eq!(cursors_columns.get(k).unwrap(), &SqlTypeMap::Text),
            "block_num" => assert_eq!(cursors_columns.get(k).unwrap(), &SqlTypeMap::BigInt),
            "block_id" => assert_eq!(cursors_columns.get(k).unwrap(), &SqlTypeMap::Text),
            _ => panic!("Invalid field name for table cursors"),
        }
    }

    // assert that primary keys are well specified
    assert_eq!(
        loader
            .get_primary_key_from_table(&"block_meta".to_string())
            .unwrap(),
        vec!["id".to_string()]
    );
    assert_eq!(
        loader
            .get_primary_key_from_table(&"cursors".to_string())
            .unwrap(),
        vec!["id".to_string()]
    );
}

#[test]
fn it_works_insert_operations() {
    let database_url = DATABASE_URL.to_string();
    let schema_namespace = String::from("public");
    let mut loader = Loader::new(database_url, schema_namespace).unwrap();

    let schema_file = PathBuf::try_from("../../tests/dbloader/sql/schema.sql").unwrap();
    loader.setup_schema(schema_file).unwrap();

    loader.load_tables().unwrap();

    let table_name = String::from("block_meta");
    let primary_key = String::from("pk1");
    let data = [
        ("parent_hash", "0x0"),
        ("timestamp", "2023-01-01"),
        ("at", "block1"),
        ("hash", "0x1"),
        ("number", "0"),
        ("id", "1"),
    ]
    .iter()
    .map(|(s, t)| (String::from(*s), String::from(*t)))
    .collect::<HashMap<String, String>>();

    assert!(loader
        .insert(table_name.clone(), primary_key, data.clone())
        .is_ok());
    assert_eq!(loader.get_entries_count(), 1);
    let entries = loader.get_entries();

    assert_eq!(entries.len(), 1);
    let ops = entries.get("block_meta").unwrap();
    let valid_op = ops.get("pk1").unwrap().clone();
    let test_op = Operation::new(
        String::from("public"),
        String::from("block_meta"),
        String::from("id"),
        OperationType::Insert,
        SqlType::Text(Text {
            inner: String::from("pk1"),
        }),
        data.iter()
            .map(|(s, t)| {
                let table = loader.get_tables().get(&table_name).unwrap();
                let sql_type = table.get(s).unwrap();
                (
                    s.clone(),
                    SqlType::parse_type(sql_type.clone(), t.clone()).unwrap(),
                )
            })
            .collect::<HashMap<String, SqlType>>(),
    );

    assert_eq!(valid_op.table_name(), test_op.table_name());
    assert_eq!(valid_op.schema_name(), test_op.schema_name());
    assert_eq!(
        valid_op.primary_key_column_name(),
        test_op.primary_key_column_name()
    );
    assert_eq!(valid_op.primary_key(), test_op.primary_key());
    assert_eq!(valid_op.op_type(), test_op.op_type());
    assert_eq!(valid_op.data(), test_op.data());

    let primary_key = String::from("pk2");
    let data = [
        ("parent_hash", "0x1"),
        ("timestamp", "2023-01-01"),
        ("at", "block2"),
        ("hash", "0x2"),
        ("number", "1"),
        ("id", "2"),
    ]
    .iter()
    .map(|(s, t)| (String::from(*s), String::from(*t)))
    .collect::<HashMap<String, String>>();

    assert!(loader
        .insert(table_name.clone(), primary_key.clone(), data.clone())
        .is_ok());
    assert_eq!(loader.get_entries_count(), 2);
    let entries = loader.get_entries();

    let ops = entries.get("block_meta").unwrap();
    assert_eq!(
        ops.get("pk2").unwrap().clone(),
        Operation::new(
            String::from("public"),
            String::from("block_meta"),
            String::from("id"),
            OperationType::Insert,
            SqlType::Text(Text {
                inner: String::from("pk2")
            }),
            data.iter()
                .map(|(s, t)| {
                    let table = loader.get_tables().get(&table_name).unwrap();
                    let sql_type = table.get(s).unwrap();
                    (
                        s.clone(),
                        SqlType::parse_type(sql_type.clone(), t.clone()).unwrap(),
                    )
                })
                .collect::<HashMap<String, SqlType>>()
        )
    );

    // inserting with same primary_key results in error
    let data = [
        ("parent_hash", "0x2"),
        ("timestamp", "2023-01-01"),
        ("at", "block3"),
        ("hash", "0x3"),
        ("number", "2"),
        ("id", "3"),
    ]
    .iter()
    .map(|(s, t)| (String::from(*s), String::from(*t)))
    .collect::<HashMap<String, String>>();

    assert!(loader.insert(table_name, primary_key, data).is_err());
}
