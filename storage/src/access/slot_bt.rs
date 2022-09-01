use crate::access::const_val::INVALID_PAGE_ID;
use crate::access::get_set_int::{get_u32, set_u32};
use crate::access::page_id::PageId;

pub struct SlotBt<'p, const LEAF: bool> {
    slice: &'p [u8],
}

pub struct SlotBtMut<'p, const LEAF: bool> {
    slice: &'p mut [u8],
}

/// slot layout
/// |--4-bytes--| payload begin offset
/// |--4-bytes--| payload data size
///                 key size for non-leaf node
///                 , or key + value size for leaf node
/// |--4-bytes--| slot capacity,
/// |--4-bytes--| page-id for non-leaf node
///                 , or tuple size for leaf node
///
const OFFSET_PAYLOAD_BEGIN: u32 = 0;
const OFFSET_SIZE: u32 = 4;
const OFFSET_FLAG: u32 = 8;
const OFFSET_PAGE_ID: u32 = 12;
const OFFSET_VALUE_SIZE: u32 = 12;
const OFFSET_LOCK_FLAG: u32 = 16;

const READ_LOCK_FLAG: u32 = 0x1;
const WRITE_LOCK_FLAG: u32 = 0x2;

pub const SLOT_SIZE: u32 = 20;
pub const LEAF_SLOT_SIZE: u32 = 20;

impl<'p, const LEAF: bool> SlotBt<'p, LEAF> {
    pub fn new(slice: &'p [u8]) -> SlotBt<'p, LEAF> {
        if slice.len() != SlotBt::<LEAF>::slot_size() as usize {
            panic!("page slot must {} bytes", SlotBt::<LEAF>::slot_size())
        }

        SlotBt { slice }
    }

    pub fn get_page_id(&self) -> PageId {
        //const_assert!(!LEAF);
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_PAGE_ID)
    }

    pub fn is_infinite(&self) -> bool {
        self.get_size() == 0 && self.get_capacity() == 0
    }

    pub fn get_offset(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_PAYLOAD_BEGIN)
    }

    pub fn get_value_size(&self) -> u32 {
        //const_assert!(LEAF);
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_VALUE_SIZE)
    }

    pub fn get_size(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_SIZE)
    }

    pub fn get_capacity(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_FLAG)
    }

    pub fn get_dead(&self) -> bool {
        self.get_size() == 0
    }

    pub fn slot_size() -> u32 {
        if LEAF {
            LEAF_SLOT_SIZE
        } else {
            SLOT_SIZE
        }
    }

    pub fn lock_flag(&self) -> u32 {
        if !LEAF {
            panic!("error");
        }
        get_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_LOCK_FLAG)
    }

    pub fn is_read_locked(&self) -> bool {
        self.lock_flag() & READ_LOCK_FLAG != 0
    }

    pub fn is_write_locked(&self) -> bool {
        self.lock_flag() & WRITE_LOCK_FLAG != 0
    }
}

impl<'p, const LEAF: bool> SlotBtMut<'p, LEAF> {
    pub fn new(slice: &'p mut [u8]) -> SlotBtMut<'p, LEAF> {
        if slice.len() != SlotBt::<LEAF>::slot_size() as usize {
            panic!("page slot must {} bytes", SlotBt::<LEAF>::slot_size())
        }

        SlotBtMut { slice }
    }

    pub fn set_capacity(&mut self, capacity: u32) {
        //const_assert!(!LEAF);
        set_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_FLAG, capacity)
    }

    pub fn set_page_id(&mut self, page_no: u32) {
        //const_assert!(!LEAF);
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            OFFSET_PAGE_ID,
            page_no,
        )
    }

    pub fn set_value_size(&mut self, value_size: u32) {
        //const_assert!(LEAF);
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            OFFSET_VALUE_SIZE,
            value_size,
        )
    }

    pub fn set_infinite(&mut self) {
        self.set_capacity(0);
        self.set_offset(0);
        self.set_size(0);
        self.set_page_id(INVALID_PAGE_ID);
    }

    pub fn set_offset(&mut self, offset: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            OFFSET_PAYLOAD_BEGIN,
            offset,
        )
    }

    pub fn set_size(&mut self, size: u32) {
        set_u32(self.slice.as_ptr(), self.slice.len(), OFFSET_SIZE, size)
    }

    pub fn set_dead(&mut self) {
        self.set_size(0)
    }

    pub fn to_slot(&'p self) -> SlotBt<'p, LEAF> {
        SlotBt::new(self.slice)
    }

    pub fn is_infinite(&self) -> bool {
        self.get_size() == 0 && self.get_offset() == 0
    }

    pub fn get_page_id(&self) -> PageId {
        let s = SlotBt::<LEAF>::new(self.slice);
        s.get_page_id()
    }

    pub fn get_offset(&self) -> u32 {
        let s = SlotBt::<LEAF>::new(self.slice);
        s.get_offset()
    }

    pub fn get_value_size(&self) -> u32 {
        let s = SlotBt::<LEAF>::new(self.slice);
        s.get_value_size()
    }

    pub fn get_size(&self) -> u32 {
        let s = SlotBt::<LEAF>::new(self.slice);
        s.get_size()
    }

    pub fn get_capacity(&self) -> u32 {
        let s = SlotBt::<LEAF>::new(self.slice);
        s.get_capacity()
    }

    pub fn get_dead(&self) -> bool {
        self.get_size() == 0
    }

    fn as_immutable(&self) -> SlotBt<LEAF> {
        SlotBt::new(self.slice)
    }

    pub fn lock_flag(&self) -> u32 {
        self.as_immutable().lock_flag()
    }

    pub fn set_lock_flag(&self, flag: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            OFFSET_LOCK_FLAG,
            flag,
        )
    }

    pub fn set_read_flag(&mut self) {
        self.set_flag(READ_LOCK_FLAG);
    }

    pub fn set_write_flag(&mut self) {
        self.set_flag(WRITE_LOCK_FLAG);
    }

    pub fn clr_read_flag(&mut self) {
        self.set_flag(!READ_LOCK_FLAG);
    }

    pub fn clr_write_flag(&mut self) {
        self.set_flag(!WRITE_LOCK_FLAG);
    }

    fn set_flag(&mut self, f: u32) {
        let flag = self.lock_flag();
        let new_flag = flag & f;
        self.set_lock_flag(new_flag);
    }

    pub fn clr_flag(&mut self) {
        self.set_lock_flag(0)
    }
}
