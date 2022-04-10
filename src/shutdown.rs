//! Shutdown signal for asynchronous tasks.

use tokio::sync::broadcast;

/// Listens for the server shutdown signal.
///
/// Only a single shutdown signal is ever sent, after which the server
/// should shutdown. This struct can be queried to check whether signal
/// has been received.
pub struct Shutdown {
    // True if the shutdown signal has been received
    shutdown: bool,

    // Channel's receiver for the shutdown signal
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Returns a new [`Shutdown`] with the given [`broadcast::Receiver`].
    ///
    /// [`Shutdown`]: opal::net::Shutdown
    /// [`broadcast::Receiver`]: tokio::sync::broadcast::Receiver
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            shutdown: false,
            notify,
        }
    }

    /// Returns `true` if a shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Blocks and waits until the shutdown signal is received, if one has
    /// not been received.
    pub async fn recv(&mut self) {
        if !self.shutdown {
            // This block is executed only once when the sender is dropped
            let _ = self.notify.recv().await;
            self.shutdown = true;
        }
    }
}
