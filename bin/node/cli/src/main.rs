use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use eureka_sink_postgres;
use std::{fs::File, io::Read};
use substreams_sink::{pb::response::Message, BlockRef, Cursor, SubstreamsSink};
use tokio_stream::StreamExt;
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/sepana.ingest.v1.rs"));
}

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
    /// SQL Schema file name (*.sql)
    #[clap(long)]
    schema_file_name: String,
    /// Postgres DSN
    #[clap(long)]
    postgres_dsn: String,
    /// Module name
    #[clap(short, long)]
    module_name: String,
    /// Start block
    #[clap(short, long, default_value = "0")]
    start_block: i64,
    /// End block
    #[clap(short, long, default_value = "0")]
    end_block: u64,
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
        || config.schema_file_name.len() == 0
        || config.postgres_dsn.len() == 0
        || (config.start_block == 0 && config.end_block == 0)
    {
        println!("Missing or invalid arguments. Use -h for help.");
        return;
    }

    let _db_loader = eureka_sink_postgres::db_loader::DBLoader::new(
        config.postgres_dsn,
        "not implemented".to_string(),
    );

    let mut client = SubstreamsSink::connect(config.firehose_endpoint, &config.package_file_name)
        .await
        .unwrap();

    println!("{:?}", client.get_package_meta());
    // todo: implement
    /*let table = db_loader.get_table(
        &client.get_package_meta().first().unwrap(),
        &config.schema_file_name,
        &config.module_name,
    )?;*/

    //let cursor = table.get_current_cursor()?;
    // todo: (later) compare to the requested range and adjust if necessary.

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
            Message::Data(data) => {
                let clock = data.clock.unwrap();
                let cursor = Cursor::new(data.cursor, BlockRef::new(clock.id, clock.number));
                println!("cursor: {:?}", cursor);
                for output in data.outputs {
                    match output.data.unwrap() {
                        substreams_sink::pb::module_output::Data::MapOutput(d) => {
                            let ops: pb::RecordChanges = decode(&d.value).unwrap();
                            println!("{}\n{:?}", d.type_url, ops);
                            // todo: implement
                            // table.insert(ops, cursor)?;
                        }
                        _ => {}
                    }
                }
                // todo: implement
                // table.flush();
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
