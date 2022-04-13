use std::fs;

use clap::Parser;
use tokio::signal;

use bitcask::{
    conf::Configuration,
    telemetry::{get_subscriber, init_subscriber},
};

/// A minimal Redis server.
#[derive(Parser)]
#[clap(name = "bitcaskd", version, author, long_about = None)]
struct Cli {
    /// Path to the configuration file without the extension.
    #[clap(short, long, default_value = "config")]
    config: String,
}

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("bitcaskd".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::parse();
    let conf = Configuration::get(&cli.config)?;

    fs::create_dir_all(&conf.storage.path)?;

    let storage = conf.storage.open()?;
    let server = conf
        .net
        .async_server(storage.get_handle(), signal::ctrl_c())
        .await?;
    server.run().await;
    Ok(())
}
