//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

pub mod command;
pub mod connection;
pub mod frame;

mod client;
mod error;
mod server;

pub use self::{client::Client, error::Error, server::Server};
