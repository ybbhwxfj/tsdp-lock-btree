use common::id::OID;

use crate::access::get_set_int::{get_u128, get_u32, set_u128, set_u32};

/// slot layout
/// |--4-bytes--| payload begin offset
/// |--4-bytes--| payload data size
/// |--4-bytes--| slot capacity,
/// |--16-bytes-| tuple_oid
///
///
pub const SLOT_HP_SIZE: u32 = 32;
const SLOT_HP_OFFSET_PAYLOAD_BEGIN: u32 = 0;
const SLOT_HP_OFFSET_SIZE: u32 = 4;
const SLOT_HP_OFFSET_CAPACITY: u32 = 8;
const SLOT_HP_OID: u32 = 12;

pub struct SlotHp<'p> {
    slice: &'p [u8],
}

pub struct SlotHpMut<'p> {
    slice: &'p mut [u8],
}

impl<'p> SlotHp<'p> {
    pub fn new(slice: &'p [u8]) -> Self {
        if slice.len() != SLOT_HP_SIZE as usize {
            panic!("page slot must {} bytes", SLOT_HP_SIZE)
        }

        SlotHp { slice }
    }

    pub fn size(&self) -> u32 {
        get_u32(self.slice.as_ptr(), self.slice.len(), SLOT_HP_OFFSET_SIZE)
    }

    pub fn capacity(&self) -> u32 {
        get_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            SLOT_HP_OFFSET_CAPACITY,
        )
    }

    pub fn offset(&self) -> u32 {
        get_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            SLOT_HP_OFFSET_PAYLOAD_BEGIN,
        )
    }

    pub fn oid(&self) -> OID {
        get_u128(self.slice.as_ptr(), self.slice.len(), SLOT_HP_OID)
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

impl<'p> SlotHpMut<'p> {
    pub fn new(slice: &'p mut [u8]) -> Self {
        if slice.len() != SLOT_HP_SIZE as usize {
            panic!("page slot must {} bytes", SLOT_HP_SIZE)
        }

        SlotHpMut { slice }
    }

    pub fn set_size(&self, size: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            SLOT_HP_OFFSET_SIZE,
            size,
        )
    }

    pub fn set_capacity(&self, capacity: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            SLOT_HP_OFFSET_CAPACITY,
            capacity,
        )
    }

    pub fn set_offset(&self, offset: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            SLOT_HP_OFFSET_PAYLOAD_BEGIN,
            offset,
        )
    }

    pub fn set_oid(&self, oid: OID) {
        set_u128(self.slice.as_ptr(), self.slice.len(), SLOT_HP_OID, oid)
    }
}
