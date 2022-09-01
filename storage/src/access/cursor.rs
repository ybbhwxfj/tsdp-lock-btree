use serde::{Deserialize, Serialize};

use crate::access::const_val::INVALID_PAGE_ID;
use crate::access::page_id::PageId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cursor {
    page_id: PageId,
    slot_no: u32,
    seq_no: u32,
}

impl Cursor {
    pub fn new() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            slot_no: 0,
            seq_no: 0,
        }
    }

    pub fn is_invalid(&self) -> bool {
        self.page_id == INVALID_PAGE_ID
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id
    }

    pub fn set_slot_no(&mut self, slot_no: u32) {
        self.slot_no = slot_no
    }

    pub fn set_seq(&mut self, seq: u32) {
        self.seq_no = seq
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn slot_no(&self) -> u32 {
        self.slot_no
    }

    pub fn seq_no(&self) -> u32 {
        self.seq_no
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self::new()
    }
}
