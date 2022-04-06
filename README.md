# Opal

`opal` is a simple key-value store that uses [Bitcask](https://riak.com/assets/bitcask-intro.pdf) as the storage engine and Redis Serialization Protocol ([RESP](https://redis.io/docs/reference/protocol-spec/)) as the network protocol.

## Supported Redis commands

+ [GET](https://redis.io/commands/get/)
+ [SET](https://redis.io/commands/set/)
+ [DEL](https://redis.io/commands/del/)

## Usages

Clone the repository and build the project. Once finish, the executables are placed in the `target/release` folder.

```bash
$ git clone https://github.com/letung3105/opal.git
$ cd opal && cargo build --release
```

Get the server help message.

```bash
$ ./target/release/svr --help

USAGE:
    svr [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help           Print help information
        --host <HOST>    The host address of the server [default: 127.0.0.1]
        --port <PORT>    The port number of the server [default: 6379]
    -V, --version        Print version information

SUBCOMMANDS:
    bitcask    Run the server using Bitcask storage engine
    help       Print this message or the help of the given subcommand(s)
    inmem      Run the server using an in-memory hashmap
    sled       Run the server using sled.rs storage engine
```

Get the client help message.

```bash
$ ./target/release/cli --help

USAGE:
    cli [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help           Print help information
        --host <HOST>    The host address of the server [default: 127.0.0.1]
        --port <PORT>    The port number of the server [default: 6379]
    -V, --version        Print version information

SUBCOMMANDS:
    del     Delete keys
    get     Get key's value
    help    Print this message or the help of the given subcommand(s)
    set     Set key's value
```
