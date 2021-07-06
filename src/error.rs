use failure::{Context, Fail};
use std::fmt::Display;
use std::io;
use std::str::Utf8Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}
#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "{}", _0)]
    IO(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    InvalidLog(String),

    #[fail(display = "{}", _0)]
    KeyNotFound(String),

    #[fail(display = "{}", _0)]
    KeyNotExist(String),

    #[fail(display = "{}", _0)]
    SerializerError(#[cause] serde_json::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error {
            inner: Context::new(ErrorKind::IO(err)),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(err: ErrorKind) -> Self {
        Error {
            inner: Context::new(err),
        }
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error {
            inner: Context::new(ErrorKind::InvalidLog(err.to_string())),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error {
            inner: Context::new(ErrorKind::SerializerError(err)),
        }
    }
}
