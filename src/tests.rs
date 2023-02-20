use std::fs;
use substreams::pb::substreams::{Modules, Request};
use tokio_stream::StreamExt;

use crate::{decode, stream_client};

#[test]
fn decode_manifest_to_modules() {
    let filename = "/Users/jorgeantonio/dev/substreams-example/substreams-ethereum-quickstart-v1.0.0.spkg".to_string();
    let contents = fs::read(filename).unwrap();
    let _manifest = decode::<Modules>(&contents).unwrap();
}

#[tokio::test]
async fn grpc_client() {
    let grpc_endpoint = "http://0.0.0.0:5000".to_string();
    let mut client = stream_client::StreamClient::connect(grpc_endpoint)
        .await
        .unwrap();

    let request = Request {
        start_block_num: 10_000,
        stop_block_num: 10_010,
        start_cursor: "".to_string(),
        fork_steps: vec![1_i32],
        irreversibility_condition: "".to_string(),
        output_modules: vec!["transfer_map".to_string()],
        modules: None,
        initial_store_snapshot_for_modules: vec![],
    };

    let stream = client.blocks(request).await.unwrap().into_inner();

    // while let Some(block) = stream.next().await {}
}
