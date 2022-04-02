use std::net::Ipv4Addr;

use structopt::StructOpt;

use opal::{
    net::Client,
    telemetry::{get_subscriber, init_subscriber},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    // Setup global `tracing` subscriber
    let subscriber = get_subscriber("opal".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let cli = Cli::from_args();

    let mut client = Client::connect(format!("{}:{}", cli.host, cli.port)).await?;
    match cli.cmd {
        Command::Set { key, value } => {
            client.set(&key, value.into()).await?;
            println!("\"OK\"");
        }
        Command::Get { key } => match client.get(&key).await? {
            Some(val) => match String::from_utf8(val.to_vec()) {
                Ok(s) => println!("\"{}\"", s),
                Err(_) => println!("{:?}", val),
            },
            None => println!("(nil)"),
        },
        Command::Del { keys } => {
            let n_deleted = client.del(&keys).await?;
            println!("(integer) {}", n_deleted);
        }
    }

    Ok(())
}

#[derive(StructOpt)]
#[structopt(name = "opal", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A key value store client")]
struct Cli {
    #[structopt(subcommand)]
    cmd: Command,

    #[structopt(
        long = "host",
        about = "Host address of the Redis server",
        default_value = "127.0.0.1"
    )]
    host: Ipv4Addr,

    #[structopt(
        long = "port",
        about = "Port address of the Redis server",
        default_value = "6379"
    )]
    port: u16,
}

#[derive(StructOpt)]
enum Command {
    #[structopt(about = "Set the value of the key")]
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        value: String,
    },

    #[structopt(about = "Get the value of the key")]
    Get {
        #[structopt(name = "KEY")]
        key: String,
    },

    #[structopt(about = "Delete the keys")]
    Del {
        #[structopt(name = "KEY")]
        keys: Vec<String>,
    },
}
