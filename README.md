# Opal

`opal` is a simple key-value store that uses [Bitcask](https://riak.com/assets/bitcask-intro.pdf) as the storage engine and Redis Serialization Protocol ([RESP](https://redis.io/docs/reference/protocol-spec/)) as the network protocol.

## Features

+ Redis-compatible server and client
+ Lock-free concurrent reads
+ Asynchronous data compaction
+ Asynchronous disk synchronization

## Usages

Clone the repository and build the project. Once finish, the executables are placed in the `target/release` folder.

```bash
$ git clone https://github.com/letung3105/opal.git
$ cd opal && cargo build --release
```

Get the server help message.

```bash
USAGE:
    svr [OPTIONS]

OPTIONS:
    -c, --config <CONFIG>    Path to the configuration file with the extension omitted.
                             Configuration can be given using different file format and the
                             application will derive the extension from the file stem [default:
                             opal]
    -h, --help               Print help information
    -V, --version            Print version information
```

Get the client help message.

```bash
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

## Configurations

To change the server settings, a configuration file is used. By default, the server will try to read the configuration file located at the directory where the server is run. Alternatively, a custom path to the configuration file can be given through the CLI upon startup. An example of the configuration file is given in [opal.toml](opal.toml).

```toml
net.host = "0.0.0.0"
net.port = 6379
net.min_backoff_ms = 500
net.max_backoff_ms = 64000
net.max_connections = 128

bitcask.path = "db"
bitcask.concurrency = 4
bitcask.readers_cache_size = 256
bitcask.max_file_size = 2000000000
bitcask.sync = "none"

bitcask.merge.policy = "always"
bitcask.merge.check_interval_ms = 180000
bitcask.merge.check_jitter = 0.3

bitcask.merge.triggers.fragmentation = 0.6
bitcask.merge.triggers.dead_bytes = 512000000

bitcask.merge.thresholds.fragmentation = 0.4
bitcask.merge.thresholds.dead_bytes = 128000000
bitcask.merge.thresholds.small_file = 10000000
```

Additionally, we can use environment variables to override the server settings. Environment variables that change the settings are prefixed with `OPAL`, and the prefix along with nested fields are separated with double underscores `__`. For example, `OPAL__NET__HOST=127.0.0.1` will change to host address to `127.0.0.1`, and `OPAL__BITCASK__MAX_FILE_SIZE=2` will change Bitcask's max file size to `2`.

## Supported Redis commands

+ [GET](https://redis.io/commands/get/)
+ [SET](https://redis.io/commands/set/)
+ [DEL](https://redis.io/commands/del/)
