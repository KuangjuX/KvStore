use failure::Fail;
use std::io;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "{}", _0)]
    IOErr(#[cause] io::Error),

    // #[fail(display = "{}", _0)]
    // StdErr(#[cause] &'static str)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOErr(err)
    }
}

// impl From<&'static str> for Error {
//     fn from(err: &'static str) -> Error {
//         Error::StdErr(err)
//     }
// }

pub type Result<T> = std::result::Result<T, Error>;