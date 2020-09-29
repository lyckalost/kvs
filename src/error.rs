use failure::Fail;
use std::io;
use std::time::SystemTimeError;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "{}", _0)]
    Io(io::Error),

    #[fail(display = "{}", _0)]
    Serde(serde_json::Error),

    #[fail(display = "{}", _0)]
    Time(std::time::SystemTimeError),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Invalid argument in the input")]
    InvalidArgument,
}

impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::Io(error)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::Serde(error)
    }
}

impl From<std::time::SystemTimeError> for KvError {
    fn from(error: SystemTimeError) -> Self {
        KvError::Time(error)
    }
}

pub type Result<T> = std::result::Result<T, KvError>;