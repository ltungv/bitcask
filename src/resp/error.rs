/// Error from parsing a message frame
#[derive(PartialEq)]
pub enum Error {
    /// Frame's data has not been fully received
    Incomplete,
    /// Frame's data does not follow specifications
    InvalidFormat,

    /// Frame's bytes could not be read as an UTF8 encoded string
    FromUtf8Error(std::string::FromUtf8Error),
}

impl std::error::Error for Error {}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::Incomplete => write!(f, "Incomplete frame"),
            Error::InvalidFormat => write!(f, "Invalid frame format"),
            Error::FromUtf8Error(e) => write!(f, "{}", e),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            Error::Incomplete => write!(f, "Incomplete frame"),
            Error::InvalidFormat => write!(f, "Invalid frame format"),
            Error::FromUtf8Error(e) => write!(f, "{}", e),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8Error(err)
    }
}
