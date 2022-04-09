use std::fs;

use tokio::net::TcpListener;
use tokio::signal;

use opal::{
    conf::Configuration,
    net::Server,
    telemetry::{get_subscriber, init_subscriber},
};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let conf = Configuration::get();

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", conf.server.host, conf.server.port)).await?;

    // Create storage
    fs::create_dir_all(&conf.bitcask.path)?;
    let storage = conf.bitcask.open()?;

    let server = Server::new(listener, storage.get_handle(), signal::ctrl_c());
    server.run().await;

    Ok(())
}
