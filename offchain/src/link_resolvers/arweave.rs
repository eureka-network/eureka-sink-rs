use std::time::Duration;

use crate::LinkResolver;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tonic::transport::Uri;

pub struct ArweaveLinkResolver {
    http_client: reqwest::Client,
}

impl ArweaveLinkResolver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http_client: reqwest::Client::builder().use_rustls_tls().build()?,
        })
    }
}

#[async_trait]
impl LinkResolver for ArweaveLinkResolver {
    async fn download(&mut self, uri: &str) -> Result<Vec<u8>> {
        let parsed_uri = uri.parse::<Uri>()?;
        let id = parsed_uri
            .host()
            .ok_or(anyhow!("Failed to parse path in {}", uri))?;
        let url = format!("https://arweave.net/{}", id);
        let content = self
            .http_client
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
