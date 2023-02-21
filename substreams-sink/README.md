# Substreams Sink

Example

```use prost::DecodeError;
use std::env;
use substreams_sink::{pb::substreams::response::Message, SubstreamsSink};
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
        .accept_compressed()
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
