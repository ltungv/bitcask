use std::io;

use thiserror::Error;

use super::{cmd, frame};

/// Error from running the server/client
#[derive(Error, Debug)]
pub enum Error {
    /// Error from the storage engine.
    #[error("Storage engine failed - {0}")]
    Storage(#[source] anyhow::Error),

    /// Error from parsing a frame.
    #[error("Frame error - {0}")]
    Frame(#[from] frame::Error),

    /// Error from parsing a commad.
    #[error("Command error - {0}")]
    Command(#[from] cmd::Error),

    /// Error from I/O operations.
    #[error("I/O error - {0}")]
    Io(#[from] io::Error),

    /// Error from running asynchronous tasks.
    #[error("Asynchronous task join error - {0}")]
    AsyncTaskJoin(#[from] tokio::task::JoinError),
}