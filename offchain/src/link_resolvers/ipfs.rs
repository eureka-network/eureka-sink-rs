use crate::LinkResolver;
use anyhow::{anyhow, Result};
use async_trait::async_trait;

pub struct IPFSLinkResolver {}

impl IPFSLinkResolver {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl LinkResolver for IPFSLinkResolver {
    async fn download(&self, _uri: &str) -> Result<Vec<u8>> {
        Err(anyhow!("not implemented"))
    }
}
