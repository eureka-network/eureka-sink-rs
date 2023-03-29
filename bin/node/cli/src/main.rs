#[macro_use]
extern crate log;
use bigdecimal::BigDecimal;
use blake2::{Blake2s256, Digest};
use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use eureka_sink_postgres::{
    cursor::CursorLoader,
    db_loader::DBLoader,
    flush::FlushLoader,
    ops::DBLoaderOperations,
    sql_types::{BigInt, Binary, Bool, ColumnValue, Decimal, Integer, Sql, Text},
};
use hex::encode;

use offchain::{
    wasm, resolver, ArweaveLinkResolver, HTTPSLinkResolver, IpfsLinkResolver, LinkResolver, ResolveTask,
    Resolver,
};
use sqlx::PgPool;
use std::{collections::HashMap, fs::File, io::Read, str::FromStr, sync::Arc};
use substreams_sink::pb;
use substreams_sink::{
    pb::Value, substreams::pb::response::Message, BlockRef, Cursor, SubstreamsSink,
};
use tokio_stream::StreamExt;
use anyhow::{anyhow, Result};

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
    /// IPFS clients
    #[clap(short, long, value_parser, num_args = 0.., value_delimiter = ' ')]
    ipfs_clients: Vec<String>,
    /// Resolver offchain data
    #[clap(short, long, default_value = "false")]
    resolve_offchain_data: bool,
    /// Maximum number of cuncurrent resolver tasks
    #[clap(long, default_value = "48")]
    max_concurrent_resolver_tasks: u64,
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

    if let Err(e) = run(config).await {
        error!("Error: {}", e);
    }
}

async fn run(config: Config) -> Result<()> {
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

    let (offchain_task_sender, wasm_host, resolver_task) = if !config.resolve_offchain_data {
        (None, None, None)
    } else {
        let mut modules: HashMap<String, &[u8]> = HashMap::new();
        modules.insert(
            config.schema.clone(),
            client
                .get_binary(&config.module_name)
                .ok_or(anyhow!("Failed to get binary"))?,
        );

        let wasm_host = wasm::Host::spawn_wasm(
            modules,
            PgPool::connect(&config.postgres_dsn).await.unwrap(),
        )
        .await
        .unwrap();

        let mut link_resolvers: HashMap<String, Arc<dyn LinkResolver>> = HashMap::new();
        link_resolvers.insert("https".to_string(), Arc::new(HTTPSLinkResolver::new()?));
        link_resolvers.insert("ar".to_string(), Arc::new(ArweaveLinkResolver::new()?));
        if config.ipfs_clients.len() > 0 {
            link_resolvers.insert(
                "ipfs".to_string(),
                Arc::new(IpfsLinkResolver::new(&config.ipfs_clients)?),
            );
        }

        let mut resolver =
            Resolver::new(&config.postgres_dsn, link_resolvers, config.max_concurrent_resolver_tasks).await?;
        let offchain_task_sender = resolver.get_sender();
        let parsers = wasm_host.get_channels().clone();
        let runtime = tokio::runtime::Handle::current();
        let resolver_task = tokio::spawn(async move {
            let _runtime_guard = runtime.enter();
            resolver.run(parsers).await
        });

        (
            Some(offchain_task_sender),
            Some(wasm_host),
            Some(resolver_task),
        )
    };

    let cursor = db_loader
        .get_cursor(config.module_name.clone())
        .or::<Cursor>(Ok(Cursor {
            cursor: "".to_string(),
            block: BlockRef {
                id: config.start_block.to_string(),
                num: u64::try_from(config.start_block).expect("Invalid start block"),
            },
        }))
        .expect("Failed to get cursor");
    let mut stream = client
        .get_stream(
            &config.module_name,
            config.start_block,
            config.end_block,
            &cursor.cursor,
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
                                            if let pb::value::Typed::Offchaindata(request) = field
                                                .new_value
                                                .as_ref()
                                                .unwrap()
                                                .typed
                                                .to_owned()
                                                .unwrap()
                                            {
                                                if let Some(ref offchain_task_sender) =
                                                    offchain_task_sender
                                                {
                                                    offchain_task_sender
                                                        .send(resolver::Message::Job(ResolveTask {
                                                            manifest: config.schema.clone(),
                                                            request,
                                                            num_retries: 0,
                                                        })).await?;
                                                }
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

    if let (Some(offchain_task_sender), Some(resolver_task), Some(wasm_host)) =
        (offchain_task_sender, resolver_task, wasm_host)
    {
        info!("Waiting for offchain content...");
        offchain_task_sender.send(resolver::Message::Termination).await?;
        let _ = resolver_task.await?;
        debug!("Waiting for WASM host...");
        wasm_host.wait().await?;
    }
    Ok(())
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
