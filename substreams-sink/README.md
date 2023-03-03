# Substreams Sink

(warning) this crate is under development and should not yet be dependend on externally as much may change.

A minimal Rust implementation of https://github.com/streamingfast/substream-sink.
This is intended to generic for any Rust project building a sink for Substreams, so it is a seperate crate.

## Build
- cargo build
  
## Tasks

- [ ] define and implement initial unit tests and possible mocks

## Example usage

```
use prost::DecodeError;
use std::env;
use substreams_sink::{pb::response::Message, SubstreamsSink};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let grpc_endpoint = env::args().nth(1).unwrap();
    let package_file_name = env::args().nth(2).unwrap();
    let module_name = env::args().nth(3).unwrap();
    let start_block = env::args().nth(4).unwrap().parse::<i64>().unwrap();
    let end_block = env::args().nth(5).unwrap().parse::<u64>().unwrap();

    let client = SubstreamsSink::connect(grpc_endpoint).await.unwrap();
    let mut stream = client
        .get_stream(
            &package_file_name,
            &module_name,
            start_block,
            end_block,
            "",
            "STEP_IRREVERSIBLE",
        )
        .await
        .unwrap()
        .into_inner();

    while let Some(resp) = stream.next().await {
        match resp.unwrap().message.unwrap() {
            Message::Data(_data) => {
                // Process BlockScopedData
            }
            _ => {}
        }
    }
}
```
