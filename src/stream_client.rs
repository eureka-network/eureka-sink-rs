use substreams::pb::substreams::{Request, Response};
use tonic::codegen::*;

#[derive(Clone, Debug)]
pub struct StreamClient<T> {
    inner: tonic::client::Grpc<T>,
}

impl StreamClient<tonic::transport::Channel> {
    // Attemps to create a new client by connecting to a given endpoint
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
        Ok(Self::new(conn))
    }
}

impl<T> StreamClient<T>
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

    pub async fn with_interceptor<F>(
        inner: T,
        interceptor: F,
    ) -> StreamClient<InterceptedService<T, F>>
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
        StreamClient::new(InterceptedService::new(inner, interceptor))
    }

    // #[must_use]
    // pub fn send_gzip(mut self) -> Self {
    //     self.inner = self.inner.send_gzip();
    //     self
    // }

    // #[must_use]
    // pub fn accept_gzip(mut self) -> Self {
    //     self.inner = self.inner.accept_gzip();
    //     self
    // }

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
        let codec = tonic::codec::ProstCodec::<Request, Response>::default();
        let path = http::uri::PathAndQuery::from_static("/sf.substreams.v1.Stream/Blocks");
        self.inner
            .server_streaming(request.into_request(), path, codec)
            .await
    }
}
