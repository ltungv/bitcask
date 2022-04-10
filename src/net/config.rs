use std::{
    io,
    net::{IpAddr, Ipv4Addr},
};

use serde::Deserialize;
use tokio::net::TcpListener;

use super::Server;

/// Network configuration
#[derive(Deserialize)]
#[serde(default)]
pub struct Config {
    /// The host address.
    pub host: IpAddr,

    /// The port number.
    pub port: u16,

    /// Max number of seconds to wait for when retrying to accept a new connection.
    /// The value is in second.
    pub max_backoff_ms: u64,

    /// Max number of concurrent connections that can be served by the server.
    pub max_connections: usize,
}

impl Config {
    /// Bind a new listener and create a server using the given storage and shutdown signal.
    pub async fn async_server<KV, S>(self, storage: KV, shutdown: S) -> io::Result<Server<KV, S>> {
        let listener = TcpListener::bind(&format!("{}:{}", self.host, self.port)).await?;
        let server = Server::new(listener, storage, shutdown);
        Ok(server)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 6379,
            max_backoff_ms: 64000,
            max_connections: 128,
        }
    }
}
