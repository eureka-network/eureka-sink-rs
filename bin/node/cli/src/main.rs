use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use std::{fs::File, io::Read};
use substreams_sink::{pb::response::Message, SubstreamsSink};
use tokio_stream::StreamExt;
pub mod ingest {
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
        || (config.start_block == 0 && config.end_block == 0)
    {
        println!("Missing or invalid arguments. Use -h for help.");
        return;
    }

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
                for output in data.outputs {
                    match output.data.unwrap() {
                        substreams_sink::pb::module_output::Data::MapOutput(d) => {
                            let ops: ingest::IngestOperations = decode(&d.value).unwrap();
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
