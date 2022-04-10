use std::fs;

use clap::Parser;
use tokio::signal;

use opal::{
    conf::Configuration,
    telemetry::{get_subscriber, init_subscriber},
};

/// A minimal Redis server.
#[derive(Parser)]
#[clap(name = "opald", version, author, long_about = None)]
struct Cli {
    /// Path to the configuration file with the extension omitted. Configuration can be given using
    /// different file format and the application will derive the extension from the file stem.
    #[clap(short, long, default_value = "opal")]
    config: String,
}

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::parse();
    let conf = Configuration::get(&cli.config)?;

    fs::create_dir_all(&conf.bitcask.path)?;

    let storage = conf.bitcask.open()?;
    let server = conf
        .net
        .async_server(storage.get_handle(), signal::ctrl_c())
        .await?;
    server.run().await;
    Ok(())
}
