use json::{object, JsonValue};

use common::result::Result;

use crate::access::const_val::FileKind;
use crate::access::get_set_int::{get_u32, set_u32};
use crate::access::page_id::PageId;

/// page layout
/// page header 64 byte
/// --- |4 byte| page id
///     |4 byte| right link page id
///     |4 byte| left link page id
///     |4 byte| page type
///     |4 byte| page size
///     |4 byte| page sequence no
pub const PAGE_HEADER_SIZE: u32 = 32;
const PAGE_ID: u32 = 0;
const RIGHT_PAGE_ID: u32 = 4;
const LEFT_PAGE_ID: u32 = 8;
const PAGE_TYPE: u32 = 12;
const PAGE_SIZE: u32 = 16;
const PAGE_SEQ_NO: u32 = 20;

pub struct PageHdr<'a> {
    vec: &'a Vec<u8>,
}

pub struct PageHdrMut<'a> {
    vec: &'a mut Vec<u8>,
}

impl<'a> PageHdr<'a> {
    pub fn from(vec: &'a Vec<u8>) -> Self {
        Self { vec }
    }

    pub fn get_left_id(&self) -> u32 {
        self.get_u32(LEFT_PAGE_ID)
    }

    pub fn get_right_id(&self) -> u32 {
        self.get_u32(RIGHT_PAGE_ID)
    }

    pub fn get_type(&self) -> FileKind {
        let n = self.get_u32(PAGE_TYPE);
        FileKind::from_u32(n)
    }

    pub fn get_size(&self) -> u32 {
        self.get_u32(PAGE_SIZE)
    }

    pub fn get_id(&self) -> u32 {
        self.get_u32(PAGE_ID)
    }

    pub fn get_seq_no(&self) -> u32 {
        self.get_u32(PAGE_SEQ_NO)
    }

    fn get_u32(&self, offset: u32) -> u32 {
        get_u32(self.vec.as_ptr(), self.vec.len(), offset)
    }

    pub fn to_json(&self) -> Result<JsonValue> {
        Ok(object! {
            "id":self.get_id(),
            "size":self.get_size(),
            "type":self.get_type().to_string(),
            "left":self.get_left_id(),
            "right":self.get_right_id(),
        })
    }
}

impl<'a> PageHdrMut<'a> {
    pub fn from(vec: &'a mut Vec<u8>) -> Self {
        Self { vec }
    }

    pub fn format(
        &mut self,
        page_id: PageId,
        left_id: PageId,
        right_id: PageId,
        page_type: FileKind,
    ) {
        self.set_id(page_id);
        self.set_left_id(left_id);
        self.set_right_id(right_id);
        self.set_type(page_type as u32);
        self.set_size(self.vec.len() as u32);
        self.set_seq_no(1);
    }

    pub fn set_id(&mut self, page_id: PageId) {
        self.set_u32(PAGE_ID, page_id);
    }

    pub fn set_left_id(&mut self, page_id: PageId) {
        self.set_u32(LEFT_PAGE_ID, page_id);
    }

    pub fn set_right_id(&mut self, page_id: PageId) {
        self.set_u32(RIGHT_PAGE_ID, page_id);
    }

    pub fn set_type(&mut self, page_type: u32) {
        self.set_u32(PAGE_TYPE, page_type);
    }

    pub fn set_size(&mut self, size: u32) {
        self.set_u32(PAGE_SIZE, size);
    }

    pub fn set_seq_no(&mut self, seq_no: u32) {
        self.set_u32(PAGE_SEQ_NO, seq_no);
    }
    pub fn get_left_id(&self) -> u32 {
        self.get_u32(LEFT_PAGE_ID)
    }

    pub fn get_right_id(&self) -> u32 {
        self.get_u32(RIGHT_PAGE_ID)
    }

    pub fn get_type(&self) -> u32 {
        self.get_u32(PAGE_TYPE)
    }

    pub fn get_size(&self) -> u32 {
        self.get_u32(PAGE_SIZE)
    }

    pub fn get_id(&self) -> u32 {
        self.get_u32(PAGE_ID)
    }

    pub fn get_seq_no(&self) -> u32 {
        self.get_u32(PAGE_SEQ_NO)
    }

    pub fn increase_seq_no(&mut self) {
        let no = PageHdr::from(self.vec).get_seq_no();
        self.set_seq_no(no + 1);
    }

    fn set_u32(&mut self, offset: u32, n: u32) {
        set_u32(self.vec.as_mut_ptr(), self.vec.len(), offset, n);
    }
    fn get_u32(&self, offset: u32) -> u32 {
        get_u32(self.vec.as_ptr(), self.vec.len(), offset)
    }
}
