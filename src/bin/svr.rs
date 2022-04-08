use std::{env, fs, net::IpAddr, path};

use bytesize::ByteSize;
use clap::{Args, Parser, Subcommand};
use tokio::net::TcpListener;
use tokio::signal;

use opal::{
    net::Server,
    storage::{BitcaskKeyValueStorage, Config, DashMapKeyValueStorage, SledKeyValueStorage},
    telemetry::{get_subscriber, init_subscriber},
};

/// A minimal Redis server
#[derive(Parser)]
#[clap(name = "opal", version, author, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    cmd: Commands,

    /// The host address of the server
    #[clap(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// The port number of the server
    #[clap(long, default_value_t = 6379)]
    port: u16,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the server using Bitcask storage engine
    Bitcask(BitcaskArgs),

    /// Run the server using sled.rs storage engine
    Sled(SledArgs),

    /// Run the server using an in-memory hashmap
    Inmem,
}

#[derive(Args)]
struct BitcaskArgs {
    /// Maximum size of the active data file
    #[clap(long)]
    max_file_size: Option<ByteSize>,

    /// Maximum number of unused bytes before triggering a merge
    #[clap(long)]
    max_dead_bytes: Option<ByteSize>,

    /// Number of concurrent reads the engine can handle
    #[clap(long)]
    concurrency: Option<usize>,

    /// Path to the database directory
    #[clap(long)]
    path: Option<path::PathBuf>,
}

#[derive(Args)]
struct SledArgs {
    /// Path to the database directory
    #[clap(long)]
    path: Option<path::PathBuf>,
}

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::parse();

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", cli.host, cli.port)).await?;

    match cli.cmd {
        Commands::Bitcask(args) => {
            let mut conf = Config::default();
            if let Some(n) = args.max_file_size {
                conf.max_file_size(n);
            }
            if let Some(n) = args.max_dead_bytes {
                conf.merge_trigger_dead_bytes(n);
            }
            if let Some(n) = args.concurrency {
                conf.concurrency(n);
            }

            let db_dir = args.path.unwrap_or(env::current_dir()?);
            fs::create_dir_all(&db_dir)?;

            let storage = BitcaskKeyValueStorage::from(conf.open(db_dir)?);
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
        Commands::Sled(args) => {
            let db_dir = args.path.unwrap_or(env::current_dir()?);
            fs::create_dir_all(&db_dir)?;

            let db = sled::Config::default().path(db_dir).open()?;
            let storage = SledKeyValueStorage::new(db);
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
        Commands::Inmem => {
            let storage = DashMapKeyValueStorage::default();
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
    }

    Ok(())
}
