use std::env;

use eureka_sink_postgres::db_loader::Loader;

#[test]
fn it_works_start_db_loader() {
    let database_url = env::var("DATABASE_URL").unwrap();
    let database_url = std::path::PathBuf::try_from(database_url).unwrap();
    let schema = String::from("my_schema");
    let loader = Loader::new(database_url, schema).unwrap();
}
