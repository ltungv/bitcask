//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

mod client;
pub mod command;
pub mod connection;
mod error;
pub mod frame;
mod server;

pub use self::{client::Client, error::Error, server::Server};
