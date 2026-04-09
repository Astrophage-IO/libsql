use std::fmt;

#[derive(Debug)]
pub enum BoltError {
    Io(std::io::Error),
    Protocol(String),
    PackStream(String),
    Engine(String),
    ConnectionClosed,
}

impl fmt::Display for BoltError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoltError::Io(e) => write!(f, "IO error: {}", e),
            BoltError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            BoltError::PackStream(msg) => write!(f, "PackStream error: {}", msg),
            BoltError::Engine(msg) => write!(f, "Engine error: {}", msg),
            BoltError::ConnectionClosed => write!(f, "Connection closed"),
        }
    }
}

impl std::error::Error for BoltError {}

impl From<std::io::Error> for BoltError {
    fn from(e: std::io::Error) -> Self {
        BoltError::Io(e)
    }
}
