use std::net::{IpAddr, Ipv4Addr};

use serde::Deserialize;

use super::Server;

/// Network configuration
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    /// The host address.
    pub host: IpAddr,

    /// The port number.
    pub port: u16,

    /// Min number of milliseconds to wait for when retrying to accept a new connection.
    pub min_backoff_ms: u64,

    /// Max number of milliseconds to wait for when retrying to accept a new connection.
    pub max_backoff_ms: u64,

    /// Max number of concurrent connections that can be served by the server.
    pub max_connections: usize,
}

impl Config {
    /// Bind a new listener and create a server using the given storage and shutdown signal.
    pub async fn async_server<KV, S>(
        self,
        storage: KV,
        shutdown: S,
    ) -> Result<Server<KV, S>, super::Error> {
        Server::new(storage, shutdown, self).await
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 6379,
            min_backoff_ms: 500,
            max_backoff_ms: 64000,
            max_connections: 128,
        }
    }
}
