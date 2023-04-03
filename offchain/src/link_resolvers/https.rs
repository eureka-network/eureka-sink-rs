use std::time::Duration;

use crate::LinkResolver;
use anyhow::Result;
use async_trait::async_trait;

/// HTTPS link resolver
pub struct HTTPSLinkResolver {
    // TODO: reuse connections
    _http_client: reqwest::Client,
}

impl HTTPSLinkResolver {
    /// Create a new HTTPS link resolver
    pub fn new() -> Result<Self> {
        Ok(Self {
            _http_client: reqwest::Client::builder().use_rustls_tls().build()?,
        })
    }
}

#[async_trait]
impl LinkResolver for HTTPSLinkResolver {
    /// Download content from the given URI
    async fn download(&self, uri: &str) -> Result<Vec<u8>> {
        let content = reqwest::Client::builder().use_rustls_tls().build()?
            .get(uri)
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
