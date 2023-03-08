pub mod pb {
    tonic::include_proto!("sf.substreams.v1");
}
use pb::{stream_client::StreamClient, Request, Response};
use tonic::{
    codegen::{http::uri::Scheme, http::uri::Uri, *},
    metadata::AsciiMetadataValue,
    service::Interceptor,
    transport::Endpoint,
    Status,
};

#[derive(Debug, Clone, PartialEq)]
pub struct BlockRef {
    pub id: String,
    pub num: u64,
}

impl BlockRef {
    pub fn new(id: String, num: u64) -> Self {
        Self { id, num }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub cursor: String,
    pub block: BlockRef,
}

impl Cursor {
    pub fn new(cursor: String, block: BlockRef) -> Self {
        Self { cursor, block }
    }
    pub fn new_blank_cursor() -> Self {
        Cursor {
            cursor: "".to_string(),
            block: BlockRef {
                id: "".to_string(),
                num: 0,
            },
        }
    }
    pub fn is_blank(&self) -> bool {
        self.cursor.len() == 0
    }
    pub fn is_equal_to(&self, other: &Self) -> bool {
        self.cursor == other.cursor
    }
    pub fn to_string(&self) -> String {
        self.cursor.clone()
    }
}

/// Adds authorization token to request header
pub struct AuthorizationTokenInjector {
    token: Option<AsciiMetadataValue>,
}

impl Interceptor for AuthorizationTokenInjector {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        Ok(request)
    }
}

pub struct SubstreamsSink<T> {
    inner: StreamClient<InterceptedService<T, AuthorizationTokenInjector>>,
    package: pb::Package,
}

impl SubstreamsSink<tonic::transport::Channel> {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub async fn connect<D: AsRef<str>>(dst: D, package_file_name: &str) -> anyhow::Result<Self>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let uri = dst.as_ref().parse::<Uri>()?;
        let (conn, api_token) = match uri.scheme().unwrap_or(&Scheme::HTTP).as_str() {
            "http" => (Endpoint::new(uri)?.connect().await?, None),
            "https" => (
                Endpoint::new(dst)?
                    .tls_config(tonic::transport::ClientTlsConfig::new())?
                    .connect()
                    .await?,
                if let Ok(v) = std::env::var("SUBSTREAMS_API_TOKEN") {
                    Some(AsciiMetadataValue::try_from(format!("Bearer {}", v))?)
                } else {
                    None
                },
            ),
            _ => panic!("invalid uri scheme"),
        };

        Ok(SubstreamsSink {
            inner: StreamClient::with_interceptor(
                conn,
                AuthorizationTokenInjector { token: api_token },
            )
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip),
            package: ::prost::Message::decode(&std::fs::read(package_file_name)?[..])?,
        })
    }

    pub fn get_package_meta(&self) -> &Vec<pb::PackageMetadata> {
        &self.package.package_meta
    }

    /// Create stream for a manifest package module.
    pub async fn get_stream(
        &mut self,
        module_name: &str,
        start_block_num: i64,
        stop_block_num: u64,
        start_cursor: &str,
        irreversibility_condition: &str,
    ) -> Result<tonic::Response<tonic::codec::Streaming<Response>>, tonic::Status> {
        let modules = self
            .package
            .modules
            .clone()
            .ok_or(Status::invalid_argument(
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
