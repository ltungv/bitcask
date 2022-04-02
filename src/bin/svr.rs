use anyhow::Context;
use opal::{
    engine::{self, BitCaskConfig, BitCaskKeyValueStore},
    net::Server,
    telemetry::{get_subscriber, init_subscriber},
};
use std::{env, fs, net::IpAddr, path};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opald".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::from_args();

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", cli.host, cli.port)).await?;

    let db_dir = cli.path.unwrap_or(env::current_dir()?);
    fs::create_dir_all(&db_dir)?;

    let storage = BitCaskKeyValueStore(
        BitCaskConfig::default()
            .concurrency(cli.nthreads)
            .open(db_dir)
            .context("could not start BitCask engine")?,
    );
    let server = Server::new(listener, storage, signal::ctrl_c());
    server.run().await;

    // match cli.typ {
    //     engine::Type::BitCask => {
    //         let db_dir = cli.path.unwrap_or(env::current_dir()?);
    //         fs::create_dir_all(&db_dir)?;

    //         let storage = BitCaskKeyValueStore(
    //             BitCaskConfig::default()
    //                 .concurrency(cli.nthreads)
    //                 .open(db_dir)
    //                 .map_err(|e| Error::new(ErrorKind::CommandFailed, e))?,
    //         );
    //         let server = Server::new(listener, storage, signal::ctrl_c());
    //         server.run().await;
    //     }
    //     engine::Type::LFS => {
    //         let db_dir = cli.path.unwrap_or(env::current_dir()?);
    //         fs::create_dir_all(&db_dir)?;

    //         let storage = LogStructuredHashTable::open(&db_dir, cli.nthreads)?;
    //         let server = Server::new(listener, storage, signal::ctrl_c());
    //         server.run().await;
    //     }
    //     engine::Type::Sled => {
    //         let db_dir = cli.path.unwrap_or(env::current_dir()?);
    //         fs::create_dir_all(&db_dir)?;

    //         let db = sled::Config::default().path(db_dir).open().unwrap();
    //         let storage = SledKeyValueStore::new(db);
    //         let server = Server::new(listener, storage, signal::ctrl_c());
    //         server.run().await;
    //     }
    //     engine::Type::InMem => {
    //         let storage = InMemoryStorage::default();
    //         let server = Server::new(listener, storage, signal::ctrl_c());
    //         server.run().await;
    //     }
    // }

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
        default_value = "bitcask"
    )]
    typ: engine::Type,

    #[structopt(long = "path", about = "Path to the directory containing the data")]
    path: Option<path::PathBuf>,

    #[structopt(
        long = "nthreads",
        about = "Number of threads used to run the key-value store",
        default_value = "4"
    )]
    nthreads: usize,
}
