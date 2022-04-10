//! Asynchronous server for the storage engine that communicates with RESP protocol.

use std::{convert::TryFrom, future::Future, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
    time,
};
use tracing::{debug, error, info};

use super::{command::Command, connection::Connection};
use crate::{shutdown::Shutdown, storage::KeyValueStorage};

/// Provide methods and hold states for a Redis server. The server will exist when `shutdown`
/// finishes, or when there's an error.
pub struct Server<KV, S> {
    listener: Listener<KV>,
    shutdown: S,
}

/// The server's runtime state that is shared across all connections.
/// This is also in charge of listening for new inbound connections.
struct Listener<KV> {
    // Database handle
    storage: KV,

    // The TCP socket for listening for inbound connection
    listener: TcpListener,

    /// Min number of milliseconds to wait for when retrying to accept a new connection.
    min_backoff_ms: u64,

    /// Max number of milliseconds to wait for when retrying to accept a new connection.
    max_backoff_ms: u64,

    // Semaphore with `MAX_CONNECTIONS`.
    //
    // When a handler is dropped, the semaphore is decremented to grant a
    // permit. When waiting for connections to close, the listener will be
    // notified once a permit is granted.
    limit_connections: Arc<Semaphore>,

    // Broacast channeling to signal a shutdown to all active connections.
    //
    // The server is responsible for gracefully shutting down active connections.
    // When a connection is spawned, it is given a broadcast receiver handle.
    // When the server wants to gracefully shutdown its connections, a `()` value
    // is sent. Each active connection receives the value, reaches a safe terminal
    // state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    // This channel ensures that the server will wait for all connections to
    // complete processing before shutting down.
    //
    // Tokio's channnels are closed when all the `Sender` handles are dropped.
    // When a connection handler is created, it is given clone of the of
    // `shutdown_complete_tx`, which is dropped when the listener shutdowns.
    // When all the listeners shut down, the channel is closed and
    // `shutdown_complete_rx.receive()` will return `None`. At this point, it
    // is safe for the server to quit.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Reads client requests and applies those to the storage.
struct Handler<KV> {
    // Database handle.
    storage: KV,

    // Writes and reads frame.
    connection: Connection,

    // The semaphore that granted the permit for this handler.
    // The handler is in charge of releasing its permit.
    limit_connections: Arc<Semaphore>,

    // Receives shut down signal.
    shutdown: Shutdown,

    // Signals that the handler finishes executing.
    _shutdown_complete: mpsc::Sender<()>,
}

impl<KV, S> Server<KV, S> {
    /// Runs the server.
    pub async fn new(storage: KV, shutdown: S, conf: super::Config) -> Result<Self, super::Error> {
        info!(?conf, "starting server");
        // Ignoring the broadcast received because one can be created by
        // calling `subscribe()` on the `Sender`
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        let listener = Listener {
            storage,
            listener: TcpListener::bind(&format!("{}:{}", conf.host, conf.port)).await?,
            min_backoff_ms: conf.min_backoff_ms,
            max_backoff_ms: conf.max_backoff_ms,
            limit_connections: Arc::new(Semaphore::new(conf.max_connections)),
            notify_shutdown,
            shutdown_complete_rx,
            shutdown_complete_tx,
        };

        Ok(Self { listener, shutdown })
    }
}

impl<KV, S> Server<KV, S>
where
    KV: KeyValueStorage,
    S: Future,
{
    /// Runs the server that exits when `shutdown` finishes, or when there's
    /// an error.
    pub async fn run(mut self) {
        // Concurrently run the tasks and blocks the current task until
        // one of the running tasks finishes. The block that is associated
        // with the task gets to run, when the task is the first to finish.
        // Under normal circumstances, this blocks until `shutdown` finishes.
        tokio::select! {
            result = self.listener.listen() => {
                if let Err(err) = result {
                    // The server has been failing to accept inbound connections
                    // for multiple times, so it's giving up and shutting down.
                    // Error occured while handling individual connection don't
                    // propagate further.
                    error!(cause = %err, "failed to accept");
                }
            }
            _ = self.shutdown => {
                info!("shutting down");
            }
        }

        // Dropping this so tasks that have called `subscribe()` will be notified for
        // shutdown and can gracefully exit.
        drop(self.listener.notify_shutdown);

        // Dropping this so there's no dangling `Sender`. Otherwise, awaiting on the
        // channel's received will block forever because we still holding the last
        // sender instance.
        drop(self.listener.shutdown_complete_tx);

        // Awaiting for all active connections to finish processing.
        self.listener.shutdown_complete_rx.recv().await;
    }
}

impl<KV> Listener<KV> {
    /// Accepts a new connection.
    ///
    /// Returns the a [`TcpStream`] on success. Retries with an exponential
    /// backoff strategy when there's an error. If the backoff time passes
    /// to maximum allowed time, returns an error.
    ///
    /// [`TcpStream`]: tokio::net::TcpStream
    async fn accept(&mut self) -> Result<TcpStream, super::Error> {
        let mut backoff = self.min_backoff_ms;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > self.max_backoff_ms {
                        return Err(err.into());
                    }
                }
            }

            // Wait for `backoff` milliseconds
            time::sleep(Duration::from_millis(backoff)).await;

            // Doubling the backoff time
            backoff <<= 1;
        }
    }
}

impl<KV> Listener<KV>
where
    KV: KeyValueStorage,
{
    async fn listen(&mut self) -> Result<(), super::Error> {
        info!("listening for new connections");

        loop {
            // Wait for a permit to become available.
            //
            // For convenient, the handle is bounded to the semaphore's lifetime
            // and when it gets dropped, it decrements the count. Because we're
            // releasing the permit in a different task from the one we acquired it
            // in, `forget()` is use to drop the semaphore handle without releasing
            // the permit at the end of this scope.
            self.limit_connections.acquire().await.unwrap().forget();

            // Accepts a new connection and retries on error. If this function
            // returns an error, it means that the server could not accept any
            // new connection and it is aborting.
            let socket = self.accept().await?;

            // Creating the handler's state for managing the new connection
            let handler = Handler {
                storage: self.storage.clone(),
                connection: Connection::new(socket),
                limit_connections: Arc::clone(&self.limit_connections),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // Handle the connection in a new task
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause=?err, "connection error");
                }
            });
        }
    }
}

impl<KV> Handler<KV>
where
    KV: KeyValueStorage,
{
    /// Process a single connection.
    ///
    /// Currently, pipelining is not implemented. See for more details at:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[tracing::instrument(skip(self))]
    async fn run(mut self) -> Result<(), super::Error> {
        // Keeps ingesting frames when the server is still running
        while !self.shutdown.is_shutdown() {
            // Awaiting for a shutdown event or a new frame
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            // No frame left means the client closed the connection, so we can
            // return with no error
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Try to parse a command out of the frame
            let cmd = Command::try_from(frame)?;
            debug!(?cmd);

            let storage = self.storage.clone();
            cmd.apply(storage, &mut self.connection, &mut self.shutdown)
                .await?;
        }
        Ok(())
    }
}

impl<KV> Drop for Handler<KV> {
    fn drop(&mut self) {
        // Releases the permit that was granted for this handler. Performing this
        // in the `Drop` implementation ensures that the permit is always
        // automatically returned when the handler finishes
        self.limit_connections.add_permits(1);
    }
}
