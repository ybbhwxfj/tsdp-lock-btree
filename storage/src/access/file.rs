use std::io::SeekFrom;
/// wrapper of async file operations
///
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{trace_span, Instrument};

use common::error_type::ET;
use common::error_type::ET::ParseError;
use common::result::{io_result, Result};

use crate::access::file_id::FileId;

pub fn file_id_to_name(file_id: FileId) -> String {
    format!("{:x}", file_id)
}

#[allow(dead_code)]
fn name_to_file_id(name: String) -> Result<FileId> {
    match u128::from_str_radix(name.as_str(), 16) {
        Ok(id) => Ok(id),
        Err(e) => Err(ParseError(e.to_string())),
    }
}

pub async fn path_file_open(base_path: &String, file_id: FileId, ext: &str) -> Result<File> {
    let file_name = format!("{}.{}", file_id_to_name(file_id), &ext);
    let path_buf = Path::new(&base_path).join(file_name);
    let path = path_buf.as_path();
    let fd = async_open_file(path).await?;
    Ok(fd)
}

pub async fn async_open_file(path: impl AsRef<Path>) -> Result<File> {
    let open_r = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .instrument(trace_span!("File::open"))
        .await;
    io_result(open_r)
}

pub async fn async_seek(file: &mut File, offset: u64) -> Result<()> {
    let r = file.seek(SeekFrom::Start(offset)).await;
    let pos = io_result(r)?;
    assert_eq!(pos, offset);
    Ok(())
}

pub async fn async_write_all(file: &mut File, src: &[u8]) -> Result<()> {
    let ok = file.write_all(src).await;
    match ok {
        Ok(()) => Ok(()),
        Err(e) => Err(ET::IOError(e.to_string())),
    }
}

pub async fn async_read_all(file: &mut File, buf: &mut [u8]) -> Result<()> {
    let r = file.read_exact(buf).await;
    let size = io_result(r)?;
    assert_eq!(size, buf.len());
    Ok(())
}

pub async fn async_flush(file: &mut File) -> Result<()> {
    let f_r = file.flush().await;
    io_result(f_r)
}
