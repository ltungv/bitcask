use std::{env, fs, net::IpAddr, path};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use opal::{
    engine::{self, BitCaskConfig, BitCaskKeyValueStore, DashMapKeyValueStore, SledKeyValueStore},
    net::Server,
    telemetry::{get_subscriber, init_subscriber},
};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let args = Args::parse();

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", args.host, args.port)).await?;

    match args.typ {
        engine::Type::BitCask => {
            let db_dir = args.path.unwrap_or(env::current_dir()?);
            fs::create_dir_all(&db_dir)?;

            let storage = BitCaskKeyValueStore::from(
                BitCaskConfig::default()
                    .concurrency(args.nthreads)
                    .open(db_dir)?,
            );
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
        engine::Type::Sled => {
            let db_dir = args.path.unwrap_or(env::current_dir()?);
            fs::create_dir_all(&db_dir)?;

            let db = sled::Config::default().path(db_dir).open()?;
            let storage = SledKeyValueStore::new(db);
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
        engine::Type::DashMap => {
            let storage = DashMapKeyValueStore::default();
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
    }

    Ok(())
}

/// A minimal Redis server
#[derive(Parser, Debug)]
#[clap(name = "opal", version, author, long_about = None)]
struct Args {
    /// The host address of the server
    #[clap(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// The port number of the server
    #[clap(long, default_value = "6379")]
    port: u16,

    /// The key-value store engine used by the server
    #[clap(long = "type", default_value = "bitcask")]
    typ: engine::Type,

    /// Path to the database directory
    #[clap(long)]
    path: Option<path::PathBuf>,

    /// Number of threads used for parallelization
    #[clap(long, default_value = "4")]
    nthreads: usize,
}
