use bigdecimal::BigDecimal;
use blake2::{Blake2s256, Digest};
use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use eureka_sink_postgres::{
    db_loader::DBLoader,
    ops::DBLoaderOperations,
    sql_types::{BigInt, Binary, Bool, ColumnValue, Decimal, Integer, Sql, Text},
};
use hex::encode;

use crate::ingest::{value, Value};
use std::{collections::HashMap, fs::File, io::Read, str::FromStr};
use substreams_sink::{pb::response::Message, BlockRef, Cursor, SubstreamsSink};
use tokio_stream::StreamExt;

pub mod ingest {
    include!(concat!(env!("OUT_DIR"), "/sepana.ingest.v1.rs"));
}

const DOMAIN_SEPARATION_LABEL: &str = "PRIMARY_KEY_INSERT_INTO";
const PRIMARY_KEY_LEN: usize = 32;

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
    #[clap(short, long, default_value = "0")]
    start_block: i64,
    /// End block
    #[clap(short, long, default_value = "0")]
    end_block: u64,
    /// Database url to establish DB connection
    #[clap(short, long)]
    database_url: String,
    /// DB schema name
    #[clap(short, long)]
    schema_name: String,
}

#[tokio::main]
async fn main() {
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
    println!("config - {:?}", config);

    // Check required parameters until the macro is supported in clap-serde-derive merge
    if config.firehose_endpoint.len() == 0
        || config.package_file_name.len() == 0
        || config.module_name.len() == 0
        || (config.start_block == 0 && config.end_block == 0)
        || config.database_url.len() == 0
        || config.schema_name.len() == 0
    {
        println!("Missing or invalid arguments. Use -h for help.");
        return;
    }

    let mut db_loader = DBLoader::new(config.database_url, config.schema_name).unwrap();

    let mut client = SubstreamsSink::connect(config.firehose_endpoint)
        .await
        .unwrap();
    let mut stream = client
        .get_stream(
            &config.package_file_name,
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
            Message::Data(data) => {
                let clock = data.clock.unwrap();
                let cursor = Cursor::new(data.cursor, BlockRef::new(clock.id, clock.number));
                println!("cursor: {:?}", cursor);
                for output in data.outputs {
                    match output.data.unwrap() {
                        substreams_sink::pb::module_output::Data::MapOutput(d) => {
                            let ops: ingest::RecordChanges = decode(&d.value).unwrap();
                            for op in &ops.record_changes {
                                let table_name = op.record.clone();
                                let id = op.id.clone();
                                let ordinal = op.ordinal;
                                // TODO: is block_height missing? 
                                let primary_key_label = format!(
                                    "{}<{}_{}>",
                                    DOMAIN_SEPARATION_LABEL, id, ordinal
                                );
                                let mut hasher = Blake2s256::new();
                                hasher.update(primary_key_label);
                                let primary_key = hasher.finalize();
                                // convert to hex representation
                                let primary_key = encode(primary_key.as_slice());
                                match op.operation {
                                    1 => {
                                        let data =
                                            op.fields.iter().fold(HashMap::new(), |mut data, field| {
                                                let col_name = field.name.clone();
                                                assert!(
                                                    field.old_value.is_none(),
                                                    "insert operation is append only"
                                                );
                                                let new_value = field.new_value.as_ref().unwrap();
                                                let new_value = match parse_type_of(new_value) {
                                                    ColumnArrayOrValue::Value(v) => v,
                                                    ColumnArrayOrValue::Array(_) => panic!("Not implemented"),
                                                };
                                                data.insert(col_name.clone(), new_value);
                                                data
                                            });
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
                            println!("{}\n{:?}", d.type_url, ops);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
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
        value::Typed::Int32(i) => {
            return ColumnArrayOrValue::Value(ColumnValue::Integer(Integer::set_inner(*i)));
        }
        // TODO: handle integer cast appropriately
        value::Typed::Uint32(i) => return ColumnArrayOrValue::Value(ColumnValue::Integer(Integer::set_inner(*i as i32))),
        value::Typed::Int64(i) => return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(*i))),
        // TODO: handle integer cast appropriately
        value::Typed::Uint64(i) => return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(*i as i64))),
        value::Typed::Bigdecimal(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::Decimal(Decimal::set_inner(BigDecimal::from_str(b.as_str()).unwrap())))
        }
        value::Typed::Bigint(b) => {
            return ColumnArrayOrValue::Value(ColumnValue::BigInt(BigInt::set_inner(b.parse::<i64>().unwrap())))
        }
        value::Typed::String(s) => return ColumnArrayOrValue::Value(ColumnValue::Text(Text::set_inner(s.clone()))),
        value::Typed::Bytes(b) => return ColumnArrayOrValue::Value(ColumnValue::Binary(Binary::set_inner(b.clone()))),
        value::Typed::Bool(b) => return ColumnArrayOrValue::Value(ColumnValue::Bool(Bool::set_inner(b.clone()))),
        value::Typed::Array(a) => return ColumnArrayOrValue::Array(a.value.iter().map(|v| parse_type_of(v)).collect::<Vec<ColumnArrayOrValue>>()),
    }
}
