use std::{env, path::PathBuf};

use eureka_sink_postgres::db_loader::Loader;

#[test]
fn it_works_start_db_loader() {
    const DATABASE_URL: &str = "postgres://dev-node:insecure-change-me-in-prod@postgres:5432/dev-node?sslmode=disable";
    let database_url = PathBuf::try_from(DATABASE_URL).unwrap();
    let schema = String::from("my_schema");
    let loader = Loader::new(database_url, schema).unwrap();

    let schema_file = PathBuf::try_from("../sql/schema.sql").unwrap();
}
