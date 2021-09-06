use failure::Fail;
use std::io;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "{}", _0)]
    IOErr(#[cause] io::Error)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOErr(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;