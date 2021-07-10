use failure::{Context, Fail};
use std::fmt::Display;
use std::io;
use std::num::TryFromIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

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
    InvalidCommand(String),

    #[fail(display = "{}", _0)]
    KeyNotFound(String),

    #[fail(display = "{}", _0)]
    SerializerError(#[cause] serde_json::Error),

    #[fail(display = "{}", _0)]
    Incomplete(String),

    #[fail(display = "{}", _0)]
    InvalidFormat(String),

    #[fail(display = "{}", _0)]
    TypeConversionFailed(TryFromIntError),

    #[fail(display = "{}", _0)]
    Utf8ConversionError(FromUtf8Error),
}
impl Error {
    pub fn key_not_found(message: String) -> Self {
        Error::from(ErrorKind::KeyNotFound(message))
    }

    pub fn invalid_command(message: String) -> Self {
        Error::from(ErrorKind::InvalidCommand(message))
    }
    pub fn as_string(&self) -> String {
        format!("{}", self)
    }
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
            inner: Context::new(ErrorKind::InvalidCommand(err.to_string())),
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

impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Self {
        Error {
            inner: Context::new(ErrorKind::TypeConversionFailed(err)),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error {
            inner: Context::new(ErrorKind::Utf8ConversionError(err)),
        }
    }
}
