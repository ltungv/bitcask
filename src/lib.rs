//! This crate contains the implementation of a simple persitence key-value store that uses
//! a log-structured storage layer with an in-memory index. A client and server are provided
//! for interacting with the key-value storage through network connections, these implement
//! Redis serialization protocol (RESP) to communicate with each other. A minimal set of Redis's
//! commands is supported.

#![deny(rust_2018_idioms)]
#![warn(missing_docs)]

mod client;
pub mod cmd;
mod connection;
mod error;
mod frame;
pub mod server;
mod shutdown;
mod storage;

pub use client::Client;
pub use cmd::Command;
pub use connection::Connection;
pub use error::Error;
pub use frame::Frame;
pub use storage::StorageEngine;

/// Default port address of the service
pub const DEFAULT_PORT: &str = "6379";