/// RDBC Error
#[derive(Debug)]
pub enum Error {
    General(String),
}

/// RDBC Result type
pub type Result<T> = std::result::Result<T, Error>;

impl From<std::fmt::Error> for Error {
    fn from(e: std::fmt::Error) -> Self {
        Error::General(e.to_string())
    }
}