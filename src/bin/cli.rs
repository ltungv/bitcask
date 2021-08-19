use structopt::StructOpt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), opal::resp::Error> {
    // Enable logging
    tracing_subscriber::fmt::try_init().unwrap();

    let opt = ClientCli::from_args();
    let addr = format!("{}:{}", opt.host, opt.port);
    let mut client = opal::resp::Client::connect(addr).await?;

    match opt.cmd {
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
struct ClientCli {
    #[structopt(subcommand)]
    cmd: Command,

    #[structopt(
        long = "host",
        about = "Host address of the Redis server",
        default_value = "127.0.0.1"
    )]
    host: String,

    #[structopt(
        long = "port",
        about = "Port address of the Redis server",
        default_value = "6379"
    )]
    port: String,
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
