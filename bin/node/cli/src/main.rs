#[macro_use]
extern crate log;
use bigdecimal::BigDecimal;
use blake2::{Blake2s256, Digest};
use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use eureka_sink_postgres::{
    db_loader::DBLoader,
    flush::FlushLoader,
    ops::DBLoaderOperations,
    sql_types::{BigInt, Binary, Bool, ColumnValue, Decimal, Integer, Sql, Text},
};
use hex::encode;

use offchain::{HTTPSLinkResolver, ArweaveLinkResolver, Resolver};
use std::{collections::HashMap, fs::File, io::Read, str::FromStr};
use substreams_sink::pb;
use substreams_sink::{
    pb::Value, substreams::pb::response::Message, BlockRef, Cursor, SubstreamsSink,
};
use tokio_stream::StreamExt;

const DOMAIN_SEPARATION_LABEL: &str = "bin.node.cli.PRIMARY_KEY_INSERT_INTO";

#[derive(Parser)]
#[clap(author, version, about)]
struct Args {
    /// Optional config file
    #[clap(short, long, default_value = "config/default.toml")]
    config_file: std::path::PathBuf,

    /// Rest of arguments
    #[clap(flatten)]
    pub config: <Config as ClapSerde>::Opt,
}

#[derive(ClapSerde, Debug)]
struct Config {
    /// Firehose endpoint
    #[clap(short, long)]
    firehose_endpoint: String,
    /// Package file name (*.pkg)
    #[clap(short, long)]
    package_file_name: String,
    /// Module name
    #[clap(short, long)]
    module_name: String,
    /// Start block
    #[clap(short, long)]
    start_block: i64,
    /// End block
    #[clap(short, long)]
    end_block: u64,
    /// Postgres database source name to establish DB connection
    #[clap(long)]
    postgres_dsn: String,
    /// DB schema name
    #[clap(long)]
    schema: String,
    /// SQL Schema file name (*.sql)
    #[clap(long)]
    schema_file_name: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut args = Args::parse();
    let config = if let Ok(mut f) = File::open(&args.config_file) {
        let mut contents = String::new();
        f.read_to_string(&mut contents).unwrap();
        match toml::from_str::<<Config as ClapSerde>::Opt>(&contents) {
            Ok(config) => Config::from(config).merge(&mut args.config),
            Err(err) => panic!("Error in configuration file:\n{}", err),
        }
    } else {
        Config::from(&mut args.config)
    };

    // Check required parameters until the macro is supported in clap-serde-derive merge
    if config.firehose_endpoint.len() == 0
        || config.package_file_name.len() == 0
        || config.module_name.len() == 0
        || config.schema_file_name.len() == 0
        || config.schema.len() == 0
        || config.postgres_dsn.len() == 0
        || (config.start_block == 0 && config.end_block == 0)
    {
        error!("Missing or invalid arguments. Use -h for help.");
        return;
    }

    // create a [`DBLoader`] instance
    let mut db_loader = DBLoader::new(config.postgres_dsn.clone(), config.schema.clone())
        .expect("Failed to create a DBLoader instance");
    // set up the db
    let file_path = std::path::PathBuf::from_str(config.schema_file_name.as_str()).expect(
        format!(
            "Failed to parse schema_file_name = {} as a path",
            config.schema_file_name
        )
        .as_str(),
    );
    db_loader.setup_schema(file_path).expect(
        format!(
            "Failed to set up schema for file name {}",
            config.schema_file_name
        )
        .as_str(),
    );
    // load all the tables metadata to the [`DBLoader`] instance
    db_loader.load_tables().expect("Failed to load tables");

    let mut client = SubstreamsSink::connect(config.firehose_endpoint, &config.package_file_name)
        .await
        .unwrap();

    let mut resolver = Resolver::new(&config.postgres_dsn)
        .await
        .expect("Failed to connect DB state")
        .with_link_resolver(
            "https".to_string(),
            Box::new(HTTPSLinkResolver::new().expect("failed to create HTTP client")),
        )
        .with_link_resolver(
            "ar".to_string(),
            Box::new(ArweaveLinkResolver::new().expect("failed to create HTTP client")),
        )
        /* todo:: enable
        .with_link_resolver(
            "ipfs".to_string(),
            Box::new(IPFSLinkResolver::new().expect("failed to create IPFS client")),
        ) */
        .with_parser(
            config.schema.clone(),
            client
                .get_binary(&config.module_name)
                .expect("Failed to load manifest binary"),
        )
        .expect("Failed to create wasm vm");

    let mut stream = client
        .get_stream(
            &config.module_name,
            config.start_block,
            config.end_block,
            "",
            "STEP_IRREVERSIBLE",
        )
        .await
        .unwrap()
        .into_inner();

    while let Some(resp) = stream.next().await {
        match resp.unwrap().message.unwrap() {
            Message::Data(block_scoped_data) => {
                let clock = block_scoped_data.clock.unwrap();
                let cursor = Cursor::new(
                    block_scoped_data.cursor,
                    BlockRef::new(clock.id, clock.number),
                );
                for output in block_scoped_data.outputs {
                    match output.data.unwrap() {
                        substreams_sink::substreams::pb::module_output::Data::MapOutput(d) => {
                            let ops: substreams_sink::pb::RecordChanges = decode(&d.value).unwrap();
                            for op in &ops.record_changes {
                                let table_name = op.record.clone();
                                let id = op.id.clone();
                                let ordinal = op.ordinal;
                                // get primary key column name
                                let primary_key_column_name =
                                    db_loader.get_primary_key_column_name(&table_name).unwrap();
                                // TODO: is block_height missing?
                                // clock.number
                                let primary_key_label =
                                    format!("{}<{}_{}>", DOMAIN_SEPARATION_LABEL, id, ordinal);
                                let mut hasher = Blake2s256::new();
                                hasher.update(primary_key_label);
                                let primary_key = hasher.finalize();
                                // convert to hex representation
                                let primary_key = encode(primary_key.as_slice());
                                match op.operation {
                                    1 => {
                                        // set data initially with primary key
                                        let mut data = HashMap::from([(
                                            primary_key_column_name,
                                            ColumnValue::Text(Text::set_inner(primary_key.clone())),
                                        )]);
                                        for field in &op.fields {
                                            let col_name = field.name.clone();
                                            assert!(
                                                field.old_value.is_none(),
                                                "insert operation is append only"
                                            );
                                            let new_value = field.new_value.as_ref().unwrap();
                                            let new_value = match parse_type_of(new_value) {
                                                ColumnArrayOrValue::Value(v) => v,
                                                ColumnArrayOrValue::Array(_) => {
                                                    panic!("Not implemented")
                                                }
                                            };
                                            data.insert(col_name.clone(), new_value);
                                            let typed = field
                                                .new_value
                                                .as_ref()
                                                .unwrap()
                                                .typed
                                                .to_owned()
                                                .unwrap();
                                            match typed {
                                                pb::value::Typed::Offchaindata(request) => {
                                                    resolver
                                                        .add_task(&config.schema, request)
                                                        .await
                                                        .expect("Failed to add task.");
                                                }
                                                _ => {}
                                            }
                                        }
                                        db_loader
                                            .insert(table_name, primary_key, data)
                                            .expect("Failed to insert data in the DB");
                                    }
                                    0 | 2 | 3 => {
                                        unimplemented!("To be implemented!")
                                    }
                                    _ => {
                                        panic!("Invalid proto value for operation enum")
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    // todo: flush is now per module output; it might make more sense per block?
                    match db_loader.flush(output.name, cursor.clone()) {
                        Ok(()) => {}
                        Err(e) => panic!("Couldn't flush operations to postgres: {}", e),
                    };
                }
                // todo: can we flush here per block?
                // db_loader.flush()
            }
            _ => {}
        }
    }
    info!("Resolving offchain content...");
    resolver.run(true).await.expect("failed to run resolver");
}

fn decode<T: std::default::Default + prost::Message>(
    buf: &Vec<u8>,
) -> Result<T, prost::DecodeError> {
    ::prost::Message::decode(&buf[..])
}

enum ColumnArrayOrValue {
    Value(ColumnValue),
    Array(Vec<ColumnArrayOrValue>),
}

fn parse_type_of(val: &Value) -> ColumnArrayOrValue {
    let typed = val.typed.as_ref().unwrap();
    match typed {
        pb::value::Typed::Int32(i) => {
            return ColumnArrayOrValue::Value(ColumnValue::Integer(Integer::set_inner(*i)));
        }
        // TODO: handle integer cast appropriately
        pb::value::Typed::Uint32(i) => {
            return ColumnArrayOrValue::Value(ColumnValue::Integer(Integer::set_inner(*i as i32)))
        }
        pb::value::Typed::Int64(i) => {
            return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(*i)))
        }
        // TODO: handle integer cast appropriately
        pb::value::Typed::Uint64(i) => {
            return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(*i as i64)))
        }
        pb::value::Typed::Bigdecimal(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::Decimal(Decimal::set_inner(
                BigDecimal::from_str(b.as_str()).unwrap(),
            )))
        }
        pb::value::Typed::Bigint(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(
                b.parse::<i64>().unwrap(),
            )))
        }
        pb::value::Typed::String(s) => {
            return ColumnArrayOrValue::Value(ColumnValue::Text(Text::set_inner(s.clone())))
        }
        pb::value::Typed::Bytes(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::Binary(Binary::set_inner(b.clone())))
        }
        pb::value::Typed::Bool(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::Bool(Bool::set_inner(b.clone())))
        }
        pb::value::Typed::Array(a) => {
            return ColumnArrayOrValue::Array(
                a.value
                    .iter()
                    .map(parse_type_of)
                    .collect::<Vec<ColumnArrayOrValue>>(),
            )
        }
        pb::value::Typed::Offchaindata(d) => {
            return ColumnArrayOrValue::Value(ColumnValue::Text(Text::set_inner(d.uri.clone())))
        }
    }
}
