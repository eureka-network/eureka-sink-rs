pub mod pb {
    tonic::include_proto!("sf.substreams.v1");
}
use pb::{stream_client::StreamClient, Request, Response};
use tonic::{codegen::*, Status};

pub struct SubstreamsSink<T> {
    inner: StreamClient<T>,
}

impl SubstreamsSink<tonic::transport::Channel> {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
        Ok(SubstreamsSink {
            inner: StreamClient::new(conn)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        })
    }

    /// Create stream for a manifest package module.
    pub async fn get_stream(
        &mut self,
        package_file_name: &str,
        module_name: &str,
        start_block_num: i64,
        stop_block_num: u64,
        start_cursor: &str,
        irreversibility_condition: &str,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Response>>, tonic::Status> {
        let pkg: pb::Package =
            ::prost::Message::decode(
                &std::fs::read(package_file_name).map_err(|e| {
                    Status::invalid_argument(format!("failed to read package: {}", e))
                })?[..],
            )
            .map_err(|e| Status::invalid_argument(format!("failed to decode package: {}", e)))?;

        let modules = pkg.modules.ok_or(Status::invalid_argument(
            "failed to find modules in package".to_string(),
        ))?;

        let request = Request {
            start_block_num,
            start_cursor: start_cursor.to_string(),
            stop_block_num,
            fork_steps: vec![],
            irreversibility_condition: irreversibility_condition.to_string(),
            output_modules: vec![module_name.to_string()],
            production_mode: true,
            debug_initial_store_snapshot_for_modules: vec![],
            modules: Some(modules),
            output_module: module_name.to_string(),
        };
        self.inner.blocks(request).await
    }
}
