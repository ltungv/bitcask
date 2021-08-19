/// Error from parsing a message frame
pub enum Error {
    /// Frame's data has not been fully received
    IncompleteFrame,
    /// Frame's data does not follow specifications
    InvalidFrame,
    /// Could not process client's command
    CommandFailed(String),
    /// Received an unexpected frame
    UnexpectedFrame,

    /// Frame's bytes could not be read as an UTF8 encoded string
    FromUtf8Error(std::string::FromUtf8Error),
    /// Error from I/O operations
    IoError(std::io::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::IncompleteFrame => write!(f, "incomplete frame"),
            Error::InvalidFrame => write!(f, "invalid frame format"),
            Error::UnexpectedFrame => write!(f, "got unexpected frame"),
            Error::CommandFailed(err) => write!(f, "{:?}", err),
            Error::FromUtf8Error(e) => write!(f, "{:?}", e),
            Error::IoError(e) => write!(f, "{:?}", e),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::IncompleteFrame => write!(f, "incomplete frame"),
            Error::InvalidFrame => write!(f, "invalid frame format"),
            Error::UnexpectedFrame => write!(f, "got unexpected frame"),
            Error::CommandFailed(err) => write!(f, "{}", err),
            Error::FromUtf8Error(e) => write!(f, "{}", e),
            Error::IoError(e) => write!(f, "{}", e),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8Error(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}
