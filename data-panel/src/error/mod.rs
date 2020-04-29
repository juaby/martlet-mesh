/// RDBC Error
#[derive(Debug)]
pub enum Error {
    General(String),
}

/// RDBC Result type
pub type Result<T> = std::result::Result<T, Error>;