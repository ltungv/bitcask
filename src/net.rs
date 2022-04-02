//! This module contains the implementation for Redis serialization protocol (RESP),
//! along with a client and a server that supports a minimal set of commands from Redis

mod client;
mod cmd;
mod connection;
mod frame;
mod server;
mod shutdown;

pub use client::{Client, ClientError};
pub use cmd::{Command, CommandError};
pub use connection::{Connection, ConnectionError};
pub use frame::{Frame, FrameError};
pub use server::{Server, ServerError};
pub use shutdown::Shutdown;
