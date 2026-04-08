use std::fmt;
use std::io;

#[derive(Debug)]
pub enum GraphError {
    IoError(io::Error),
    PagerError(String),
    CorruptPage(u32),
    InvalidPageNumber(u32),
    NoTransaction,
    TransactionActive,
    InvalidMagic,
    UnsupportedVersion(u32),
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "I/O error: {e}"),
            Self::PagerError(msg) => write!(f, "Pager error: {msg}"),
            Self::CorruptPage(pgno) => write!(f, "Corrupt page: {pgno}"),
            Self::InvalidPageNumber(pgno) => write!(f, "Invalid page number: {pgno}"),
            Self::NoTransaction => write!(f, "No active transaction"),
            Self::TransactionActive => write!(f, "Transaction already active"),
            Self::InvalidMagic => write!(f, "Invalid graph file magic bytes"),
            Self::UnsupportedVersion(v) => write!(f, "Unsupported format version: {v}"),
        }
    }
}

impl std::error::Error for GraphError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for GraphError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}
