use std::{env, fs};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::{error::SubstreamsSinkPostgresError, stream_client::StreamClient};

mod error;
mod stream_client;
#[cfg(test)]
mod tests;

/// Substreams manifest
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Manifest {
    #[prost(string, tag = "1")]
    pub spec_version: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub modules: ::prost::alloc::vec::Vec<substreams::pb::substreams::Module>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub modules_code: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}

pub struct SubstreamsSinkPostgres {
    filename: String,
    grpc_endpoint: String,
}

impl SubstreamsSinkPostgres {
    pub fn new(filename: String, grpc_endpoint: String) -> Self {
        Self {
            filename,
            grpc_endpoint,
        }
    }

    pub async fn get_grpc_stream_client(
        &self,
    ) -> Result<StreamClient<Channel>, SubstreamsSinkPostgresError> {
        StreamClient::connect(self.grpc_endpoint.clone())
            .await
            .map_err(|e| SubstreamsSinkPostgresError::TonicError(e))
    }

    fn get_file_contents(&self) -> Result<Vec<u8>, SubstreamsSinkPostgresError> {
        fs::read(self.filename.clone()).map_err(|e| SubstreamsSinkPostgresError::IoError(e))
    }

    pub fn get_request(
        &self,
        start_block_num: i64,
        start_cursor: String,
        stop_block_num: u64,
        fork_steps: Vec<i32>,
        irreversibility_condition: String,
        initial_store_snapshot_for_modules: Vec<String>,
        output_modules: Vec<String>,
    ) -> Result<tonic::Request<substreams::pb::substreams::Request>, SubstreamsSinkPostgresError>
    {
        let contents = self.get_file_contents()?;
        let modules = substreams::proto::decode::<substreams::pb::substreams::Modules>(&contents)
            .map_err(|e| SubstreamsSinkPostgresError::DecodeError(e))?;
        Ok(tonic::Request::new(substreams::pb::substreams::Request {
            start_block_num,
            start_cursor,
            stop_block_num,
            fork_steps,
            irreversibility_condition,
            initial_store_snapshot_for_modules,
            modules: Some(modules),
            output_modules,
        }))
    }
}

#[tokio::main]
#[allow(dead_code)]
async fn main() {
    let filename = env::args().nth(1).unwrap();
    let grpc_endpoint = env::args().nth(2).unwrap();

    let contents = fs::read(filename).expect("Couldn't read the file!");
    let modules =
        substreams::proto::decode::<substreams::pb::substreams::Modules>(&contents).unwrap();

    let request = substreams::pb::substreams::Request {
        start_block_num: 6810706,
        start_cursor: "".to_string(),
        stop_block_num: 6810806,
        fork_steps: vec![],
        irreversibility_condition: "".to_string(),
        modules: Some(modules),
        output_modules: vec!["block_to_tokens".to_string()],
        initial_store_snapshot_for_modules: vec![],
    };

    let mut client = StreamClient::connect(grpc_endpoint).await.unwrap();
    let request = tonic::Request::new(request);

    let mut stream = client.blocks(request).await.unwrap().into_inner();

    while let Some(resp) = stream.next().await {
        match resp.unwrap().message.unwrap() {
            substreams::pb::substreams::response::Message::Progress(_) => {
                // TODO: print message
            }
            substreams::pb::substreams::response::Message::SnapshotData(_) => {
                // TODO: print message
            }
            substreams::pb::substreams::response::Message::SnapshotComplete(_) => {
                // TODO: print message
            }
            substreams::pb::substreams::response::Message::Data(_data) => {
                // TODO: insert data in PostGresSQL table
            }
        }
    }
}
