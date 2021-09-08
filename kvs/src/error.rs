use failure::Fail;
use std::io;
use serde_json;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    IOErr(#[cause] io::Error),

    // #[fail(display = "{}", _0)]
    // StdErr(#[cause] &'static str)
    #[fail(display = "{}", _0)]
    SerdeErr(#[cause] serde_json::Error)
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IOErr(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::SerdeErr(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;