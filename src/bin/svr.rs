use opal::net::Server;
use std::net::IpAddr;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> Result<(), opal::net::Error> {
    tracing_subscriber::fmt::try_init().unwrap();

    let cli = Cli::from_args();
    let port = cli.port;
    let host = cli.host;

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", host, port)).await?;

    let server = Server::new(listener, signal::ctrl_c());
    server.run().await;

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
}
