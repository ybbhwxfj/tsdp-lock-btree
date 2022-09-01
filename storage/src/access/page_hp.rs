use std::slice::{from_raw_parts, from_raw_parts_mut};

use json::{array, object, JsonValue};

use common::error_type::ET;
use common::id::OID;
use common::result::Result;

use crate::access::cap_of::capacity_of;
use crate::access::const_val::FileKind;
use crate::access::get_set_int::{get_u32, set_u32};
use crate::access::page_hdr::{PageHdr, PageHdrMut, PAGE_HEADER_SIZE};
use crate::access::page_id::PageId;
use crate::access::slot_hp::{SlotHp, SlotHpMut, SLOT_HP_SIZE};
use crate::access::to_json::ToJson;

/// btree page layout
/// page header 20 byte
/// btree page header 44 byte
///     |4 byte| slot count
///     |4 byte| slot active count
///     |4 byte| data offset start
///     |......| reserve
/// page body | page size - page header size |
///     /----array of slot, indexed from start ----|
///     /----empty space ----|
///     /----array of data(heap tuple), indexed from end.----|

const PAGE_HP_HEADER_SIZE: u32 = PAGE_HEADER_SIZE + 44;
const PAGE_HP_OFF_COUNT: u32 = PAGE_HEADER_SIZE + 4;
const PAGE_HP_OFF_ACTIVE_COUNT: u32 = PAGE_HEADER_SIZE + 8;
const PAGE_HP_OFF_DATA_OFFSET: u32 = PAGE_HEADER_SIZE + 12;

pub struct PageHeap<'a> {
    vec: &'a Vec<u8>,
}

pub struct PageHeapMut<'a> {
    vec: &'a mut Vec<u8>,
}

fn slot_offset_of_index(index: u32) -> usize {
    let offset = (index * SLOT_HP_SIZE) as usize + PAGE_HP_HEADER_SIZE as usize;
    return offset;
}

pub fn page_heap_get_slot(vec: &Vec<u8>, index: u32) -> SlotHp {
    let offset = slot_offset_of_index(index);
    let s = unsafe { from_raw_parts(vec.as_ptr().add(offset as usize), SLOT_HP_SIZE as usize) };
    SlotHp::new(s)
}

pub fn page_heap_get_slot_mut(vec: &mut Vec<u8>, index: u32) -> SlotHpMut {
    let offset = slot_offset_of_index(index);
    let s =
        unsafe { from_raw_parts_mut(vec.as_mut_ptr().add(offset as usize), SLOT_HP_SIZE as usize) };
    SlotHpMut::new(s)
}

fn page_heap_get_tuple(vec: &Vec<u8>, index: u32) -> (OID, &[u8]) {
    let slot = page_heap_get_slot(vec, index);
    let oid = slot.oid();
    let offset = slot.offset();
    let length = slot.size();
    let slice = unsafe { from_raw_parts(vec.as_ptr().add(offset as usize), length as usize) };
    (oid, slice)
}

impl<'a> PageHeap<'a> {
    pub fn from(vec: &'a Vec<u8>) -> Self {
        Self { vec }
    }

    pub fn get_count(&self) -> u32 {
        self.get_value(PAGE_HP_OFF_COUNT)
    }

    pub fn data_begin(&self) -> u32 {
        self.get_value(PAGE_HP_OFF_DATA_OFFSET)
    }

    pub fn get_active_count(&self) -> u32 {
        self.get_value(PAGE_HP_OFF_ACTIVE_COUNT)
    }

    pub fn get_slot(&self, index: u32) -> SlotHp {
        page_heap_get_slot(self.vec, index)
    }

    pub fn get_tuple(&self, index: u32) -> (OID, &[u8]) {
        page_heap_get_tuple(self.vec, index)
    }

    fn get_data_begin(&self) -> u32 {
        self.get_value(PAGE_HP_OFF_DATA_OFFSET)
    }

    pub fn to_json<T: ToJson>(&self, tuple_to_json: &T) -> Result<JsonValue> {
        let json_hdr = self.header().to_json()?;
        let count = self.get_count();
        let mut tuples = array![];
        let mut slots = array![];
        for i in 0..count {
            let slot = self.get_slot(i);
            if slot.is_empty() {
                continue;
            }
            slots
                .push(object! {
                    "oid":slot.oid().to_string(),
                    "offset":slot.offset(),
                    "capacity":slot.capacity(),
                    "size":slot.size(),
                })
                .expect("json array push error");

            let (_, tuple) = self.get_tuple(i);
            let tuple_json = tuple_to_json.to_json(tuple)?;
            tuples.push(tuple_json).expect("json array push error");
        }
        let j = object! {
            "pg_hdr" : json_hdr,
            "hp_hdr" : {
                "count" : count,
                "active_count" : self.get_active_count(),
                "data_begin" : self.get_data_begin(),
            },
            "slots" : slots,
            "tuples" : tuples,
        };
        Ok(j)
    }

    fn slot_end(&self) -> u32 {
        self.get_count() * SLOT_HP_SIZE + PAGE_HP_HEADER_SIZE
    }

    #[allow(dead_code)]
    fn slot_offset(index: u32) -> u32 {
        index * SLOT_HP_SIZE
    }

    fn get_value(&self, offset: u32) -> u32 {
        get_u32(self.vec.as_ptr(), self.vec.len(), offset)
    }

    fn header(&self) -> PageHdr {
        PageHdr::from(self.vec)
    }

    pub fn will_update_ok(&self, index: u32, tuple: &[u8]) -> Result<bool> {
        let slot = self.get_slot(index);
        let size = tuple.len() as u32;
        let cap = capacity_of(size);
        let count = self.get_count();
        if index >= count {
            return Err(ET::OutOffIndex);
        }
        if slot.capacity() < capacity_of(cap) {
            return Ok(false);
        }
        Ok(true)
    }
}

impl<'a> PageHeapMut<'a> {
    pub fn from(vec: &'a mut Vec<u8>) -> Self {
        Self { vec }
    }

    fn as_immutable(&self) -> PageHeap {
        PageHeap::from(self.vec)
    }

    pub fn format(&mut self, page_id: PageId, left_id: PageId, right_id: PageId) {
        let mut header = PageHdrMut::from(self.vec);
        header.format(page_id, left_id, right_id, FileKind::FileHeap);
        self.set_count(0);
        self.set_active_count(0);
        self.set_data_begin(self.vec.len() as u32);
    }
    pub fn get_count(&self) -> u32 {
        self.as_immutable().get_count()
    }

    pub fn data_begin(&self) -> u32 {
        self.as_immutable().data_begin()
    }

    pub fn get_active_count(&self) -> u32 {
        self.as_immutable().get_active_count()
    }

    pub fn get_slot_mut(&mut self, index: u32) -> SlotHpMut {
        page_heap_get_slot_mut(self.vec, index)
    }

    pub fn get_slot(&self, index: u32) -> SlotHp {
        page_heap_get_slot(self.vec, index)
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn get_tuple(&self, index: u32) -> (OID, &[u8]) {
        page_heap_get_tuple(self.vec, index)
    }

    fn set_data_begin(&mut self, offset: u32) {
        self.set_value(PAGE_HP_OFF_DATA_OFFSET, offset);
    }

    fn set_count(&mut self, count: u32) {
        self.set_value(PAGE_HP_OFF_COUNT, count);
    }

    fn set_active_count(&mut self, count: u32) {
        self.set_value(PAGE_HP_OFF_ACTIVE_COUNT, count);
    }

    fn slot_end(&self) -> u32 {
        self.as_immutable().slot_end()
    }

    #[allow(dead_code)]
    fn slot_offset(index: u32) -> u32 {
        index * SLOT_HP_SIZE
    }

    fn set_value(&mut self, offset: u32, value: u32) {
        set_u32(self.vec.as_mut_ptr(), self.vec.len(), offset, value)
    }

    /// put a tuple to page
    /// return :
    ///     option slot index of if success
    ///     None if fail on capacity
    #[cfg_attr(debug_assertions, inline(never))]
    pub fn put(&mut self, oid: OID, tuple: &[u8]) -> Option<u32> {
        let count = self.get_count();
        let slot_end = self.slot_end();
        let data_begin = self.data_begin();
        let size = tuple.len() as u32;
        let cap = capacity_of(size);
        if slot_end + cap > data_begin {
            return None;
        }
        let offset = data_begin - cap;

        let slot = self.get_slot_mut(count);

        slot.set_size(size);
        slot.set_capacity(cap);
        slot.set_oid(oid);
        slot.set_offset(offset);

        self.set_data_begin(offset);
        self.set_count(count + 1);
        self.set_active_count(self.get_active_count() + 1);
        self.mem_copy(offset, tuple);

        Some(count)
    }

    pub fn update(&mut self, index: u32, oid: Option<OID>, tuple: &[u8]) {
        let offset = {
            let slot = self.get_slot(index);
            slot.offset()
        };
        let slot_mut = self.get_slot_mut(index);
        match oid {
            Some(id) => {
                slot_mut.set_oid(id);
            }
            None => {}
        }
        let size = tuple.len() as u32;
        slot_mut.set_size(size);
        slot_mut.set_offset(offset);
        self.mem_copy(offset, tuple);
    }

    pub fn remove(&mut self, index: u32) -> Result<OID> {
        let count = self.get_count();
        if index >= count {
            return Err(ET::OutOffIndex);
        }
        let s = self.get_slot(index);
        let oid = s.oid();

        let slot = self.get_slot_mut(index);
        slot.set_size(0);
        slot.set_oid(0);

        self.set_active_count(self.get_active_count() - 1);
        Ok(oid)
    }

    #[allow(dead_code)]
    fn header(&self) -> PageHdr {
        PageHdr::from(self.vec)
    }

    fn mem_copy(&mut self, offset: u32, data: &[u8]) {
        if offset as usize + data.len() > self.vec.len() {
            panic!("error")
        }
        unsafe {
            std::ptr::copy(
                data.as_ptr(),
                self.vec.as_mut_ptr().add(offset as usize),
                data.len(),
            )
        }
    }
}
