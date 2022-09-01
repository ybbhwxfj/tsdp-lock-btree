use std::io;
use std::path::Path;

use project_root::get_project_root;
use serde::de::DeserializeOwned;
use serde::Serialize;

use common::error_type::ET;
use common::result::{io_result, Result};

#[allow(dead_code)]
#[cfg(test)]
pub fn test_case_file_path(case_name: String) -> Result<String> {
    let mut path_buf = io_result(get_project_root())?;

    // this file
    // ../data path
    let p = Path::new(file!()).parent().unwrap().parent().unwrap();

    path_buf = path_buf.join(p).join("data").join(case_name);
    let s = match path_buf.as_path().to_str() {
        Some(s) => s.to_string(),
        None => {
            return Err(ET::NoneOption);
        }
    };
    Ok(s)
}

#[allow(dead_code)]
#[cfg(test)]
pub fn json_write<W, T>(writer: W, value: &T) -> Result<()>
where
    W: io::Write,
    T: ?Sized + Serialize,
{
    match serde_json::to_writer_pretty(writer, value) {
        Ok(()) => {}
        Err(e) => {
            return Err(ET::JSONError(e.to_string()));
        }
    }
    Ok(())
}

#[allow(dead_code)]
#[cfg(test)]
pub fn json_read<R, T>(read: R) -> Result<T>
where
    R: io::Read,
    T: DeserializeOwned,
{
    let ret = match serde_json::from_reader(read) {
        Ok(v) => v,
        Err(e) => {
            return Err(ET::JSONError(e.to_string()));
        }
    };
    return Ok(ret);
}
