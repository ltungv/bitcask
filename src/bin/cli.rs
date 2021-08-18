use std::net::SocketAddr;
use structopt::StructOpt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), opal::resp::Error> {
    // Enable logging
    tracing_subscriber::fmt::try_init().unwrap();

    let opt = ClientCli::from_args();
    match opt.cmd {
        Command::Set { key, val } => {
            unimplemented!();
        }
        Command::Get { key } => {
            let mut client = opal::resp::Client::connect(opt.addr).await?;
            match client.get(&key).await? {
                Some(val) => match String::from_utf8(val.to_vec()) {
                    Ok(s) => println!("\"{}\"", s),
                    Err(_) => println!("{:?}", val),
                },
                None => println!("(nil)"),
            }
        }
        Command::Rm { key } => {
            unimplemented!()
        }
    }
    Ok(())
}

#[derive(StructOpt)]
struct ClientCli {
    #[structopt(subcommand)]
    cmd: Command,

    #[structopt(
        long = "addr",
        about = "IP address of the key-value store",
        default_value = "127.0.0.1:4000"
    )]
    addr: SocketAddr,
}

#[derive(StructOpt)]
enum Command {
    #[structopt(about = "Set a value to a key in the key-value store")]
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        val: String,
    },

    #[structopt(about = "Get a value from a key in the key-value store")]
    Get {
        #[structopt(name = "KEY")]
        key: String,
    },

    #[structopt(about = "Remove a key from the key-value store")]
    Rm {
        #[structopt(name = "KEY")]
        key: String,
    },
}
