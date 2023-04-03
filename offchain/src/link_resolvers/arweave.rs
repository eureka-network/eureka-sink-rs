use std::time::Duration;

use crate::LinkResolver;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tonic::transport::Uri;

/// Arweave link resolver
pub struct ArweaveLinkResolver {
    // TODO: reuse connections
    _http_client: reqwest::Client,
}

impl ArweaveLinkResolver {
    /// Create a new Arweave link resolver
    pub fn new() -> Result<Self> {
        Ok(Self {
            _http_client: reqwest::Client::builder().use_rustls_tls().build()?,
        })
    }
}

#[async_trait]
impl LinkResolver for ArweaveLinkResolver {
    /// Download content from the given URI
    async fn download(&self, uri: &str) -> Result<Vec<u8>> {
        let parsed_uri = uri.parse::<Uri>()?;
        let id = parsed_uri
            .host()
            .ok_or(anyhow!("Failed to parse path in {}", uri))?;
        let url = format!("https://arweave.net/{}", id);
        debug!("fetching {}", url);
        let content = reqwest::Client::builder()
            .use_rustls_tls()
            .build()?
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .text()
            .await?;
        debug!("downloaded {}", content);
        Ok(content.as_bytes().to_vec())
        // todo: pass string
    }
}
