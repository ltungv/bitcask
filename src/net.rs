//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

pub(crate) mod client;
pub(crate) mod cmd;
pub(crate) mod connection;
pub(crate) mod frame;
pub(crate) mod server;
pub(crate) mod shutdown;

pub use client::{Client, ClientError};
pub use server::{Server, ServerError};
