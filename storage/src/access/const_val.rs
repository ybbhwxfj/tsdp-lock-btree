use std::fmt::{Debug, Display, Formatter, Result};
use std::mem;

use common::id::OID;

use crate::access::file_id::FileId;
use crate::access::page_id::PageId;

pub const TUPLE_HEADER_SIZE: u32 = 4;

pub const TUPLE_SIZE_OFFSET: u32 = 0;
pub const TUPLE_NEXT_OFFSET: u32 = 4;

pub const INVALID_PAGE_ID: PageId = PageId::MAX;
pub const INVALID_SEGMENT_ID: FileId = OID::MAX;

pub const FILE_TYPE_CLOG: u32 = 1;
pub const FILE_PAGE_TYPE_BTREE: u32 = 2;
pub const FILE_PAGE_TYPE_HEAP: u32 = 3;

pub const FILE_EXT_CLOG: &str = "clog";
pub const FILE_EXT_BTREE: &str = "bt";
pub const FILE_EXT_HEAP: &str = "hp";

#[derive(Debug, Eq, PartialEq)]
pub enum FileKind {
    FileHeap = FILE_PAGE_TYPE_HEAP as isize,
    FileBTree = FILE_PAGE_TYPE_BTREE as isize,
}

const EXT_NAME: [&str; 4] = ["", FILE_EXT_CLOG, FILE_EXT_BTREE, FILE_EXT_HEAP];

pub fn file_type_to_ext(file_type: FileKind) -> &'static str {
    let i = file_type as usize;
    assert!(i != 0 && i < EXT_NAME.len());
    EXT_NAME[i]
}

impl Display for FileKind {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:?}", self)
    }
}

impl FileKind {
    pub fn from_u32(n: u32) -> FileKind {
        unsafe { mem::transmute(n as u8) }
    }
}
