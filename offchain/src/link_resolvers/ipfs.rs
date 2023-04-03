use crate::LinkResolver;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tonic::transport::Uri;

#[allow(dead_code)]
enum IpfsClientState {
    Online = 0,
    Timeout = 1,
}
#[allow(dead_code)]
struct IpfsClient {
    address: String,
    state: IpfsClientState,
    last_state_update: Instant,
    // stats
}

/// IPFS link resolver
pub struct IpfsLinkResolver {
    // TODO: reuse connections
    _http_client: reqwest::Client,
    clients: Vec<IpfsClient>,
}

impl IpfsLinkResolver {
    /// Create a new IPFS link resolver
    pub fn new(clients: &Vec<String>) -> Result<Self> {
        Ok(Self {
            _http_client: reqwest::Client::builder().use_rustls_tls().build()?,
            clients: clients
                .iter()
                .map(|address| IpfsClient {
                    address: address.clone(),
                    state: IpfsClientState::Online,
                    last_state_update: Instant::now(),
                })
                .collect(),
        })
    }

    /// Get the best client to use
    fn get_best_client(&self) -> Option<&IpfsClient> {
        // todo: implement client stats
        self.clients.first()
    }
}

#[async_trait]
impl LinkResolver for IpfsLinkResolver {
    /// Download content from the given URI
    async fn download(&self, uri: &str) -> Result<Vec<u8>> {
        if let Some(client) = self.get_best_client() {
            let parsed_uri = uri.parse::<Uri>()?;
            let cid = parsed_uri
                .host()
                .ok_or(anyhow!("Failed to parse path in {}", uri))?;
            let url = format!("{}/api/v0/cat?arg={}", &client.address, cid);
            debug!("fetching {}", url);
            let content = reqwest::Client::builder()
                .use_rustls_tls()
                .build()?
                .post(&url)
                .timeout(Duration::from_secs(5))
                .send()
                .await?
                .text()
                .await?;
            debug!("downloaded {}", content);
            Ok(content.as_bytes().to_vec())
        } else {
            Err(anyhow!("No ipfs client"))
        }
    }
}
