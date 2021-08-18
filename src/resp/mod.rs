//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

mod client;
mod connection;
mod error;
mod frame;

mod cmd;

pub use client::Client;
pub use connection::Connection;
pub use error::Error;
pub use frame::Frame;
