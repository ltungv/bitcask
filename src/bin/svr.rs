use opal::{
    engine::{self, InMemoryStorage, KvStore},
    net::Server,
    telemetry::{get_subscriber, init_subscriber},
};
use std::{env, fs, net::IpAddr, path};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> Result<(), opal::error::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::from_args();

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", cli.host, cli.port)).await?;
    match cli.typ {
        engine::Type::LFS => {
            let db_dir = cli.path.unwrap_or(env::current_dir()?);
            fs::create_dir_all(&db_dir)?;

            let storage = KvStore::open(&db_dir)?;
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
        engine::Type::Memory => {
            let storage = InMemoryStorage::default();
            let server = Server::new(listener, storage, signal::ctrl_c());
            server.run().await;
        }
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = "opal", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A key value store server")]
struct Cli {
    #[structopt(
        long = "host",
        about = "Host address of the Redis server",
        default_value = "127.0.0.1"
    )]
    host: IpAddr,

    #[structopt(
        long = "port",
        about = "Port address of the Redis server",
        default_value = "6379"
    )]
    port: u16,

    #[structopt(
        long = "type",
        about = "The engine used by the key-value store",
        default_value = "lfs"
    )]
    typ: engine::Type,

    #[structopt(long = "path", about = "Path to the directory containing the data")]
    path: Option<path::PathBuf>,
}
