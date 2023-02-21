pub mod pb;
use crate::pb::substreams::{Request, Response};
use tonic::{codegen::*, Status};

pub struct SubstreamsSink<T> {
    inner: tonic::client::Grpc<T>,
}

impl SubstreamsSink<tonic::transport::Channel> {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
        Ok(Self::new(conn))
    }
}

impl<T> SubstreamsSink<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub fn new(inner: T) -> Self {
        let inner = tonic::client::Grpc::new(inner);
        Self { inner }
    }
    pub fn with_interceptor<F>(inner: T, interceptor: F) -> SubstreamsSink<InterceptedService<T, F>>
    where
        F: tonic::service::Interceptor,
        T::ResponseBody: Default,
        T: tonic::codegen::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<
                <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
            >,
        >,
        <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
            Into<StdError> + Send + Sync,
    {
        SubstreamsSink::new(InterceptedService::new(inner, interceptor))
    }

    /// Create stream for a manifest package.
    pub async fn get_stream(
        &mut self,
        package_file_name: &str,
        module_name: &str,
        start_block_num: i64,
        stop_block_num: u64,
        start_cursor: &str,
        irreversibility_condition: &str,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Response>>, tonic::Status> {
        let pkg: pb::substreams::Package = decode(
            &std::fs::read(package_file_name)
                .map_err(|_| Status::invalid_argument("failed to read package".to_string()))?,
        )
        .map_err(|_| Status::invalid_argument("failed to decode package".to_string()))?;

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
        self.blocks(request).await
    }

    pub fn send_compressed(mut self) -> Self {
        self.inner = self.inner.send_compressed(CompressionEncoding::Gzip);
        self
    }

    pub fn accept_compressed(mut self) -> Self {
        self.inner = self.inner.accept_compressed(CompressionEncoding::Gzip);
        self
    }

    pub async fn blocks(
        &mut self,
        request: impl tonic::IntoRequest<Request>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Response>>, tonic::Status> {
        self.inner.ready().await.map_err(|e| {
            tonic::Status::new(
                tonic::Code::Unknown,
                format!("Service was not ready: {}", e.into()),
            )
        })?;
        let codec = tonic::codec::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static("/sf.substreams.v1.Stream/Blocks");
        self.inner
            .server_streaming(request.into_request(), path, codec)
            .await
    }
}

fn decode<T: std::default::Default + prost::Message>(
    buf: &Vec<u8>,
) -> Result<T, prost::DecodeError> {
    ::prost::Message::decode(&buf[..])
}
