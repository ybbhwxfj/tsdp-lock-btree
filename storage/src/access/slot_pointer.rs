use crate::access::const_val::INVALID_PAGE_ID;
use crate::access::get_set_int::{get_u32, set_u32};
use crate::access::page_id::PageId;

const POINTER_SIZE: usize = 8;
const OFF_PAGE_ID: u32 = 0;
const OFF_SLOT_NO: u32 = 4;

// reference a slice located at a page
// can be converted to SlotPointer
pub struct SlotPointerRef<'a> {
    slice: &'a [u8],
}

pub struct SlotPointMutRef<'a> {
    slice: &'a mut [u8],
}

impl<'a> SlotPointerRef<'a> {
    pub fn len() -> usize {
        POINTER_SIZE as usize
    }

    pub fn from(slice: &'a [u8]) -> Self {
        if slice.len() < POINTER_SIZE {
            panic!("ERROR")
        }

        return Self { slice };
    }

    pub fn page_id(&self) -> PageId {
        get_u32(self.slice.as_ptr(), POINTER_SIZE, OFF_PAGE_ID)
    }

    pub fn slot_no(&self) -> u32 {
        get_u32(self.slice.as_ptr(), POINTER_SIZE, OFF_SLOT_NO)
    }

    pub fn is_invalid(&self) -> bool {
        self.page_id() == INVALID_PAGE_ID
    }

    pub fn vec(&self) -> Vec<u8> {
        Vec::from(self.slice)
    }
}

impl<'a> SlotPointMutRef<'a> {
    pub fn from(slice: &'a mut [u8]) -> Self {
        if slice.len() < POINTER_SIZE {
            panic!("ERROR")
        }
        return Self { slice };
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        set_u32(self.slice.as_mut_ptr(), POINTER_SIZE, OFF_PAGE_ID, page_id);
    }

    pub fn set_slot_no(&mut self, no: u32) {
        set_u32(self.slice.as_mut_ptr(), POINTER_SIZE, OFF_SLOT_NO, no);
    }
}

pub struct SlotPointer {
    page_id: PageId,
    slot_no: u32,
}

impl SlotPointer {
    pub fn new(page_id: PageId, slot_no: u32) -> Self {
        SlotPointer { page_id, slot_no }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn slot_no(&self) -> u32 {
        self.slot_no
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.resize(SlotPointerRef::len(), 0u8);
        let mut p = SlotPointMutRef::from(vec.as_mut_slice());
        p.set_page_id(self.page_id);
        p.set_slot_no(self.slot_no);
        vec
    }

    pub fn to_binary(&self, s: &mut [u8]) {
        if s.len() < SlotPointerRef::len() {
            panic!("error length")
        }
        let mut p = SlotPointMutRef::from(s);
        p.set_page_id(self.page_id);
        p.set_slot_no(self.slot_no);
    }
}
