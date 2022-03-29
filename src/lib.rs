//! This crate contains the implementation of a simple persitence key-value store that uses
//! a log-structured storage layer with an in-memory index. A client and server are provided
//! for interacting with the key-value storage through network connections, these implement
//! Redis serialization protocol (RESP) to communicate with each other. A minimal set of Redis's
//! commands is supported.

#![deny(rust_2018_idioms)]
#![deny(rust_2021_compatibility)]
#![warn(missing_docs)]

pub mod engine;
pub mod net;
