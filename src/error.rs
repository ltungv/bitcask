//! Errors that occur when using the key-value store

/// Error from parsing a message frame
#[derive(Debug)]
pub struct Error {
    repr: Repr,
}

impl Error {
    /// Create a new error
    pub fn new<E>(kind: ErrorKind, error: E) -> Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let error = error.into();
        Self {
            repr: Repr::Custom(Box::new(CustomRepr { kind, error })),
        }
    }

    /// Get the underlying error kind. Returns `None` if the error was propagated from
    /// third party sources
    pub fn kind(&self) -> Option<ErrorKind> {
        match self.repr {
            Repr::Simple(ref kind) => Some(*kind),
            Repr::Custom(ref repr) => Some(repr.kind),
            _ => None,
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self.repr {
            Repr::Simple(ref kind) => write!(f, "{}", kind.as_str()),
            Repr::Custom(ref repr) => write!(f, "{}: {}", repr.kind.as_str(), repr.error),
            Repr::FromUtf8Error(ref e) => write!(f, "{}", e),
            Repr::IoError(ref e) => write!(f, "{}", e),
            Repr::BincodeError(ref e) => write!(f, "{}", e),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Simple(kind),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self {
            repr: Repr::FromUtf8Error(err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            repr: Repr::IoError(err),
        }
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self {
            repr: Repr::BincodeError(err),
        }
    }
}

/// Different custom errors returned by the `net` module
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Could not process client's command
    CommandFailed,
    /// Frame's data has not been fully received
    IncompleteFrame,
    /// Frame's data does not follow specifications
    InvalidFrame,
    /// Received an unexpected frame
    UnexpectedFrame,
    /// Operation on a non-existent key
    KeyNotFound,
    /// Faulty on-disk log
    CorruptedLog,
    /// Faulty in-memory index
    CorruptedIndex,
}

impl ErrorKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match *self {
            Self::CommandFailed => "command failed",
            Self::IncompleteFrame => "incomplete frame",
            Self::InvalidFrame => "invalid frame",
            Self::UnexpectedFrame => "unexpected frame",
            Self::KeyNotFound => "key not found",
            Self::CorruptedLog => "corrupted log",
            Self::CorruptedIndex => "corrupted index",
        }
    }
}

#[derive(Debug)]
enum Repr {
    Simple(ErrorKind),
    Custom(Box<CustomRepr>),

    /// Frame's bytes could not be read as an UTF8 encoded string
    FromUtf8Error(std::string::FromUtf8Error),
    /// Error from I/O operations
    IoError(std::io::Error),
    /// Bincode error
    BincodeError(bincode::Error),
}

#[derive(Debug)]
struct CustomRepr {
    kind: ErrorKind,
    error: Box<dyn std::error::Error + Send + Sync>,
}
