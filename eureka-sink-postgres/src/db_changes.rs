use crate::{error::SubstreamsSinkPostgresError, SubstreamsSinkPostgres};
use diesel::{allow_tables_to_appear_in_same_query, Connection, PgConnection, RunQueryDsl};
use std::{
    collections::HashMap,
    fs::create_dir_all,
    hash::Hash,
    ops::Add,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use substreams_database_change::pb::database::DatabaseChanges;

// pub struct Loader {
//     database: String,
//     schema: String,
//     entries: HashMap<String, HashMap<String, Operation>>,
//     entries_count: u64,
//     tables: HashMap<String, HashMap<String, String>>,
//     table_primary_keys: HashMap<String, String>,
// }

pub struct PostgresSinker {
    connection: PgConnection,
}

impl PostgresSinker {
    pub fn try_create(path: PathBuf) -> Result<Self, SubstreamsSinkPostgresError> {
        create_dir_all(path.parent().unwrap())
            .map_err(|_| SubstreamsSinkPostgresError::FileSystemPathDoesNotExist)?;

        let database_url = path.to_str().expect("database_url utf-8 error");
        let connection = PgConnection::establish(database_url)
            .map_err(|e| SubstreamsSinkPostgresError::ConnectionError(e))?;

        Ok(Self {
            connection: connection,
        })
    }

    pub fn get_connection(&self) -> &PgConnection {
        &self.connection
    }
}

pub fn apply_database_changes(
    db_changes: DatabaseChanges,
    sinker: PostgresSinker,
) -> Result<(), SubstreamsSinkPostgresError> {
    let connection = sinker.get_connection();

    for change in db_changes.table_changes {
        let table = change.table;
        let pk = change.pk;

        let mut changes = HashMap::new();

        for field in change.fields {
            changes.insert(field.name.clone(), field.new_value.clone());
        }

        let sql_query = match change.operation {
            0 => {
                // Unsert
            }
            1 => {
                // Create
                format!("INSERT INTO {table} where ");
            }
            2 => {
                // Update
            }
            3 => {
                // Delete
            }
            _ => panic!("Should not get this code"),
        };

        // connection.build_transaction().read_write().run(|conn| {
        //     let write_attempt = diesel::sql_query(sql_query).load(conn)?;
        //     Ok(())
        // });
    }
    Ok(())
}
