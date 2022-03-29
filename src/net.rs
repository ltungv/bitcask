//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

mod client;
mod cmd;
mod connection;
mod frame;
mod server;
mod shutdown;

pub use client::Client;
pub use connection::Connection;
pub use frame::Frame;
pub use server::Server;
pub use shutdown::Shutdown;
