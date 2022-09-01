use serde::Serialize;

use crate::access::file_id::FileId;

pub type PageId = u32;

#[derive(Serialize, Eq, Hash, Clone, Debug, Copy)]
pub struct PageIndex {
    page_id: PageId,
    file_id: FileId,
}

impl PartialEq<Self> for PageIndex {
    fn eq(&self, other: &Self) -> bool {
        self.page_id.eq(&other.page_id) && self.file_id.eq(&other.file_id)
    }
}

pub fn page_id_to_offset(page_id: PageId, page_size: u64) -> u64 {
    page_size * (page_id as u64)
}

impl PageIndex {
    pub fn new(file_id: FileId, page_id: PageId) -> PageIndex {
        PageIndex { page_id, file_id }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn to_string(&self) -> String {
        format!("f{}p{}", self.file_id, self.page_id)
    }
}
