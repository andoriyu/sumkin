use snafu::{Snafu};

pub type SumkinResult<T> = Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database backend error: {}", source))]
    BackendError { source: sqlx::Error },

    #[snafu(display("I/O error: {}", source))]
    IoError { source: std::io::Error },
}

impl From<sqlx::Error> for Error {
    fn from(source: sqlx::Error) -> Error {
        Error::BackendError { source }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Error {
        Error::IoError { source }
    }
}
