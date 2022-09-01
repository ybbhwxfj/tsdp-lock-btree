use std::num::ParseIntError;

use crate::error_type::ET;

pub type Result<T> = std::result::Result<T, ET>;

pub fn io_result<T>(r: std::result::Result<T, std::io::Error>) -> Result<T> {
    match r {
        Ok(t) => Ok(t),
        Err(e) => Err(ET::IOError(e.to_string())),
    }
}

pub fn parse_int_result<T>(r: std::result::Result<T, ParseIntError>) -> Result<T> {
    match r {
        Ok(t) => Ok(t),
        Err(e) => Err(ET::ParseError(e.to_string())),
    }
}
