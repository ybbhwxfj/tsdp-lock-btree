use std::cmp::Ordering;
use std::ops::Bound;

use json::{array, object, JsonValue};
use log::error;

use adt::compare::Compare;
use adt::range::RangeBounds;
use adt::slice::Slice;
use common::error_type::ET;

use crate::access::access_opt::TxOption;
use crate::access::bt_key_raw::BtKeyRaw;
use crate::access::cap_of::capacity_of;
use crate::access::const_val::{FileKind, INVALID_PAGE_ID};
use crate::access::get_set_int::{get_u32, set_u32};
use crate::access::modify_list::{ModifyList, ParentModifyOp};

use crate::access::page_hdr::{PageHdr, PageHdrMut, PAGE_HEADER_SIZE};
use crate::access::page_id::PageId;

use crate::access::slot_bt::{SlotBt, SlotBtMut};
use crate::access::to_json::ToJson;
use crate::access::tuple_raw::TupleRaw;
use crate::access::upsert::{ResultUpsert, Upsert};

/// btree page layout
/// page header 20 byte
/// btree page header 44 byte
///     |4 byte| key count
///     |4 byte| key active count
///     |4 byte| data offset start
///     |4 byte| page level
///              We considered the leaf level to be the lowest level(started from 0).
///     |32 byte| reserve
/// page body | page size - page header size |
///     /----array of slot, indexed from start ----|
///     /----empty space ----|
///     /----array of data(key for non-leaf node, key-value for leaf node), indexed from end.----|

pub const BT_PAGE_HEADER_SIZE: u32 = 64;

const BT_KEY_COUNT: u32 = PAGE_HEADER_SIZE + 0;
const BT_KEY_ACTIVE: u32 = PAGE_HEADER_SIZE + 4;
const BT_DATA_START: u32 = PAGE_HEADER_SIZE + 8;
const BT_PAGE_LEVEL: u32 = PAGE_HEADER_SIZE + 20;

pub const LEAF_PAGE: bool = true;
pub const NON_LEAF_PAGE: bool = false;

pub struct PageHdrBt<'a> {
    vec: &'a Vec<u8>,
}

pub struct PageHdrBtMut<'a> {
    vec: &'a mut Vec<u8>,
}

pub struct PageBTree<'a, const IS_LEAF: bool> {
    vec: &'a Vec<u8>,
}

pub struct PageBTreeMut<'a, const IS_LEAF: bool> {
    vec: &'a mut Vec<u8>,
}

pub enum LBResult {
    /// found the key
    /// u32: An index to the first element in the range [first, last) that is not less than
    /// bool: The found index key is equal to the search key
    Found(u32, bool),
    NotFound,
}

pub fn is_leaf_page(guard: &Vec<u8>) -> bool {
    let level = get_u32(guard.as_ptr(), guard.len(), BT_PAGE_LEVEL);
    return level == 0;
}

pub fn bt_key_cmp_empty_as_max<C: Compare<[u8]>>(cmp: &C, k1: &[u8], k2: &[u8]) -> Ordering {
    if k1.len() == 0 && k2.len() == 0 {
        Ordering::Equal
    } else if k1.len() == 0 {
        Ordering::Greater
    } else if k2.len() == 0 {
        Ordering::Less
    } else {
        cmp.compare(k1, k2)
    }
}

fn page_bt_get_slot<const IS_LEAF: bool>(vec: &Vec<u8>, index: u32) -> SlotBt<IS_LEAF> {
    let hdr = PageHdrBt::from(vec);
    let count = hdr.get_count();
    if count <= index {
        panic!("exceed size");
    }

    let offset = PageBTree::<IS_LEAF>::slot_offset(index);
    let slice = unsafe {
        let s = vec.as_ptr().wrapping_add(offset as usize);
        std::slice::from_raw_parts(s, SlotBt::<IS_LEAF>::slot_size() as usize)
    };
    SlotBt::new(slice)
}

#[allow(dead_code)]
fn page_bt_get_payload<const IS_LEAF: bool>(vec: &Vec<u8>, index: u32) -> &[u8] {
    let slot: SlotBt<IS_LEAF> = page_bt_get_slot(vec, index);
    let offset = slot.get_offset();
    let size = slot.get_size();
    let slice = unsafe {
        let s = vec.as_ptr().wrapping_add(offset as usize);
        std::slice::from_raw_parts(s, size as usize)
    };
    slice
}

fn page_bt_get_slice(vec: &Vec<u8>, offset: u32, size: u32) -> &[u8] {
    let slice = unsafe {
        let s = vec.as_ptr().wrapping_add(offset as usize);
        std::slice::from_raw_parts(s, size as usize)
    };
    slice
}

fn page_bt_get_key<const IS_LEAF: bool>(vec: &Vec<u8>, index: u32) -> &[u8] {
    let slot: SlotBt<IS_LEAF> = page_bt_get_slot(vec, index);
    let offset = slot.get_offset();
    let size = slot.get_size();
    assert!((size as usize) < vec.len());
    let data = page_bt_get_slice(vec, offset, size);
    if IS_LEAF {
        let key_size = if size == 0 {
            0
        } else {
            (size - slot.get_value_size()) as usize
        };
        let ks = &data[0..key_size];
        //trace!("get value[{} {}]", offset, key_size);
        ks
    } else {
        //trace!("get value[{} {}]", offset, size);
        data
    }
}

fn page_bt_get_value<const IS_LEAF: bool>(vec: &Vec<u8>, index: u32) -> &[u8] {
    let slot: SlotBt<IS_LEAF> = page_bt_get_slot(vec, index);
    let offset = slot.get_offset();
    let mut value_size = slot.get_value_size();
    let size = slot.get_size();
    if size == 0 {
        value_size = 0;
    }
    //trace!("get value[{} {}]", offset, size);
    let data = page_bt_get_slice(vec, offset + size - value_size, value_size);
    //const_assert!(LEAF);
    data
}

impl<'a> PageHdrBt<'a> {
    pub fn from(vec: &'a Vec<u8>) -> Self {
        Self { vec }
    }

    pub fn is_leaf(&self) -> bool {
        self.get_page_level() == 0
    }

    pub fn get_right_id(&self) -> PageId {
        PageHdr::from(self.vec).get_right_id()
    }

    pub fn get_left_id(&self) -> PageId {
        PageHdr::from(self.vec).get_left_id()
    }

    pub fn get_count(&self) -> u32 {
        self.get_u32(BT_KEY_COUNT)
    }

    pub fn get_type(&self) -> FileKind {
        PageHdr::from(self.vec).get_type()
    }

    pub fn get_active_count(&self) -> u32 {
        self.get_u32(BT_KEY_ACTIVE)
    }

    pub fn get_slot_end(&self) -> u32 {
        if self.get_page_level() == 0 {
            self.get_count() * SlotBt::<LEAF_PAGE>::slot_size() + BT_PAGE_HEADER_SIZE
        } else {
            self.get_count() * SlotBt::<NON_LEAF_PAGE>::slot_size() + BT_PAGE_HEADER_SIZE
        }
    }

    pub fn get_data_begin(&self) -> u32 {
        self.get_u32(BT_DATA_START)
    }

    pub fn get_page_size(&self) -> u32 {
        PageHdr::from(self.vec).get_size()
    }

    pub fn get_page_level(&self) -> u32 {
        self.get_u32(BT_PAGE_LEVEL)
    }

    pub fn get_free_space(&self) -> u32 {
        let s = self.get_slot_end();
        let e = self.get_data_begin();
        return e - s;
    }

    fn get_u32(&self, offset: u32) -> u32 {
        get_u32(self.vec.as_ptr(), self.vec.len(), offset)
    }
}

impl<'a> PageHdrBtMut<'a> {
    pub fn from(vec: &'a mut Vec<u8>) -> Self {
        Self { vec }
    }

    pub fn set_right_id(&mut self, page_id: PageId) {
        PageHdrMut::from(self.vec).set_right_id(page_id);
    }

    pub fn set_left_id(&mut self, page_id: PageId) {
        PageHdrMut::from(self.vec).set_left_id(page_id);
    }

    pub fn set_count(&mut self, count: u32) {
        self.set_u32(BT_KEY_COUNT, count);
    }

    pub fn set_type(&mut self, page_type: u32) {
        PageHdrMut::from(self.vec).set_type(page_type)
    }

    pub fn set_active_count(&mut self, active_count: u32) {
        self.set_u32(BT_KEY_ACTIVE, active_count);
    }

    pub fn set_data_begin(&mut self, data_start: u32) {
        self.set_u32(BT_DATA_START, data_start);
    }

    pub fn set_page_size(&mut self, page_size: u32) {
        PageHdrMut::from(self.vec).set_size(page_size);
    }

    pub fn set_page_level(&mut self, page_level: u32) {
        self.set_u32(BT_PAGE_LEVEL, page_level);
    }

    fn set_u32(&mut self, offset: u32, n: u32) {
        set_u32(self.vec.as_mut_ptr(), self.vec.len(), offset, n)
    }
}

impl<'a, const IS_LEAF: bool> PageBTree<'a, IS_LEAF> {
    pub fn from(vec: &'a Vec<u8>) -> Self {
        assert_eq!(is_leaf_page(vec), IS_LEAF);
        Self { vec }
    }

    pub fn is_leaf(&self) -> bool {
        self.get_page_level() == 0
    }

    pub fn get_high_key_vec(&self) -> Option<Vec<u8>> {
        match self.get_last_key() {
            Some(k) => Some(Vec::from(k)),
            None => None,
        }
    }

    pub fn contain<C, K>(&self, range: &RangeBounds<K>, cmp: &C) -> bool
    where
        C: Compare<[u8]>,
        K: Slice,
    {
        let left_ok = match range.low() {
            Bound::Included(k) | Bound::Excluded(k) => {
                if self.get_left_id() == INVALID_PAGE_ID {
                    true
                } else if self.get_count() > 1 {
                    let key = self.get_key(0);
                    if bt_key_cmp_empty_as_max(cmp, k.as_slice(), key).is_le() {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Bound::Unbounded => self.get_left_id() == INVALID_PAGE_ID,
        };

        let right_ok = match range.up() {
            Bound::Included(k) | Bound::Excluded(k) => {
                if self.get_right_id() == INVALID_PAGE_ID {
                    true
                } else if self.get_count() > 1 {
                    let last_index = self.get_count() - 1;
                    let key = self.get_key(last_index);
                    if bt_key_cmp_empty_as_max(cmp, k.as_slice(), key).is_ge() {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Bound::Unbounded => self.get_right_id() == INVALID_PAGE_ID,
        };

        return left_ok && right_ok;
    }

    pub fn get_id(&self) -> PageId {
        PageHdr::from(self.vec).get_id()
    }

    pub fn get_seq_no(&self) -> u32 {
        PageHdr::from(self.vec).get_seq_no()
    }

    pub fn get_right_id(&self) -> PageId {
        PageHdr::from(self.vec).get_right_id()
    }

    pub fn get_left_id(&self) -> PageId {
        PageHdr::from(self.vec).get_left_id()
    }

    pub fn get_count(&self) -> u32 {
        PageHdrBt::from(self.vec).get_count()
    }

    pub fn get_type(&self) -> FileKind {
        PageHdr::from(self.vec).get_type()
    }

    pub fn get_active_count(&self) -> u32 {
        PageHdrBt::from(self.vec).get_active_count()
    }

    pub fn get_slot_end(&self) -> u32 {
        self.get_count() * SlotBt::<IS_LEAF>::slot_size() + BT_PAGE_HEADER_SIZE
    }

    pub fn get_data_begin(&self) -> u32 {
        PageHdrBt::from(self.vec).get_data_begin()
    }

    pub fn get_page_size(&self) -> u32 {
        PageHdr::from(self.vec).get_size()
    }

    pub fn get_page_level(&self) -> u32 {
        PageHdrBt::from(self.vec).get_page_level()
    }

    pub fn get_free_space(&self) -> u32 {
        let s = self.get_slot_end();
        let e = self.get_data_begin();
        return e - s;
    }

    pub fn get_slot(&self, index: u32) -> SlotBt<IS_LEAF> {
        page_bt_get_slot(self.vec, index)
    }

    pub fn slot_offset(index: u32) -> u32 {
        let offset = index * SlotBt::<IS_LEAF>::slot_size() + BT_PAGE_HEADER_SIZE;
        offset
    }

    pub fn get_key(&self, index: u32) -> &[u8] {
        page_bt_get_key::<IS_LEAF>(self.vec, index)
    }

    pub fn get_value(&self, index: u32) -> &[u8] {
        page_bt_get_value::<IS_LEAF>(self.vec, index)
    }

    #[allow(dead_code)]
    fn get_payload(&self, offset: u32, size: u32) -> &[u8] {
        let slice = unsafe {
            let s = self.vec.as_ptr().wrapping_add(offset as usize);
            std::slice::from_raw_parts(s, size as usize)
        };
        slice
    }

    /// Returns LBResult
    /// Find the first element greater or equal than ${k}
    #[cfg_attr(debug_assertions, inline(never))]
    pub fn lower_bound<K, C: Compare<[u8]>>(&self, key: &K, cmp: &C) -> LBResult
    where
        K: Slice,
    {
        let count = self.get_count();
        let mut low = 0;
        let mut high = count;
        while high > low {
            let slot = low + (high - low) / 2;
            let to_check = self.get_key(slot);
            let order = bt_key_cmp_empty_as_max(cmp, to_check, key.as_slice());
            match order {
                Ordering::Equal => {
                    high = slot;
                }
                Ordering::Less => {
                    low = slot + 1;
                }
                Ordering::Greater => {
                    high = slot;
                }
            }
        }
        if high == count {
            LBResult::NotFound
        } else {
            let to_check = self.get_key(high);
            let order = bt_key_cmp_empty_as_max(cmp, to_check, key.as_slice());
            LBResult::Found(high, order.is_eq())
        }
    }

    pub fn opt_high_key(&self) -> Option<Vec<u8>> {
        match self.get_last_key() {
            Some(k) => Some(Vec::from(k)),
            None => None,
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn get_first_key(&self) -> Option<&[u8]> {
        let index = self.get_count();
        if index == 0 {
            None
        } else {
            let key = self.get_key(0);
            Some(key)
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn get_last_key(&self) -> Option<&[u8]> {
        let index = self.get_count();
        if index == 0 {
            None
        } else {
            let key = self.get_key(index - 1);
            Some(key)
        }
    }
    fn opt_high_key_for_delete(
        &self,
        index: u32,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let count = self.get_count();
        if count == index + 1 {
            if count >= 2 {
                let old_key = self.get_key(count - 1);
                let new_key = self.get_key(count - 2);
                let op = (
                    ParentModifyOp::ParentUpdateSub,
                    Some(Vec::from(old_key)),
                    Some(Vec::from(new_key)),
                );
                Some(op)
            } else {
                let old_key = self.get_key(count - 1);
                let op = (
                    ParentModifyOp::ParentDeleteSub,
                    Some(Vec::from(old_key)),
                    None,
                );
                Some(op)
            }
        } else {
            None
        }
    }

    fn opt_high_key_for_update<K: Slice>(
        &self,
        index: u32,
        key: &K,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let count = self.get_count();
        if count == index + 1 {
            let k = self.get_key(count - 1);
            let op = (
                ParentModifyOp::ParentUpdateSub,
                Some(Vec::from(k)),
                Some(Vec::from(key.as_slice())),
            );
            Some(op)
        } else {
            None
        }
    }

    fn opt_high_key_for_insert<K: Slice>(
        &self,
        index: u32,
        key: &K,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let count = self.get_count();
        if index == count {
            let op = if count > 1 {
                let k = self.get_key(count - 1);
                (
                    ParentModifyOp::ParentUpdateSub,
                    Some(Vec::from(k)),
                    Some(Vec::from(key.as_slice())),
                )
            } else {
                // need create
                (
                    ParentModifyOp::ParentInsertSub,
                    None,
                    Some(Vec::from(key.as_slice())),
                )
            };
            Some(op)
        } else {
            None
        }
    }

    pub fn split_position<const LEAF: bool>(
        &self,
        new_add_index: u32,
        new_add_size: u32,
    ) -> Result<(u32, Option<(u32, bool)>), ET> {
        assert_eq!(LEAF, self.is_leaf());
        //  split higher half of keys to new_page
        let max = self.get_count();
        assert!(new_add_index <= max);
        let mut left_count = max / 2;
        let mut stutter_cnt = 0;
        let slot_end = self.get_slot_end();
        let data_begin = self.get_data_begin();
        let page_size = self.get_page_size();
        while left_count != 0 && left_count != max && stutter_cnt < max {
            stutter_cnt += 1;
            let slot = self.get_slot(left_count - 1);
            // left_cnt == slot_array1.len()
            // max - left_cnt = slot_array2.len()
            // |----page-header----|
            // |slot_array1 slot_array2 ---- data_array2 data_array1|
            let (test_left, test_right) = if new_add_index == left_count {
                (true, true)
            } else if new_add_index < left_count {
                (true, false)
            } else {
                (false, true)
            };
            let off = slot.get_offset();
            let slot_size = SlotBt::<LEAF>::slot_size();
            let left_empty_begin = BT_PAGE_HEADER_SIZE + left_count * slot_size;
            let right_emtpy_begin = BT_PAGE_HEADER_SIZE + (max - left_count) * slot_size;
            let left_empty_end = off;
            let right_empty_end = page_size - (off - data_begin);
            let left_empty = left_empty_end - left_empty_begin;
            let right_empty = right_empty_end - right_emtpy_begin;

            // trace!("split:{}, empty space left {} {}, right {} {}",
            //    left_count,left_empty, test_left, right_empty, test_right);
            assert_eq!(
                left_empty + right_empty,
                data_begin - slot_end + page_size - BT_PAGE_HEADER_SIZE
            );
            if test_left {
                assert!(new_add_index <= left_count);
                if left_empty >= new_add_size {
                    return Ok((left_count, Some((new_add_index, true))));
                }
            }
            if test_right {
                assert!(new_add_index >= left_count);
                if right_empty >= new_add_size {
                    return Ok((left_count, Some((new_add_index - left_count, false))));
                }
            }
            if test_left {
                left_count -= 1;
            } else {
                left_count += 1;
            }
        }
        if left_count == 0 && new_add_index == 0 {
            return Ok((0, Some((0, true))));
        } else if left_count == max && new_add_index == max {
            return Ok((max, Some((0, false))));
        }

        // no available capacity even after a split
        return Ok((max / 2, None));
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn to_json<T: ToJson, VT: ToJson>(
        &self,
        context: &T,
        value2json: &VT,
    ) -> Result<JsonValue, ET> {
        if self.is_leaf() {
            self.page_to_json::<T, VT>(context, value2json)
        } else {
            self.page_to_json::<T, VT>(context, value2json)
        }
    }

    pub fn page_meta_to_json(&self) -> Result<JsonValue, ET> {
        let json_obj = object! {
            "page_id": self.get_id(),
            "left_link": self.get_left_id(),
            "right_link": self.get_right_id(),
            "page_type": self.get_type().to_string(),
            "count": self.get_count(),
            "active_key_count":self.get_active_count(),
            "slot_offset_end": self.get_slot_end(),
            "data_offset_begin": self.get_data_begin(),
            "page_level": self.get_page_level(),
        };
        return Ok(json_obj);
    }

    pub fn page_to_json<KT: ToJson, VT: ToJson>(
        &self,
        context: &KT,
        value2json: &VT,
    ) -> Result<JsonValue, ET> {
        let n = self.get_count();
        let mut slot_array = array![];
        let mut key_value_array = array![];
        for i in 0..n {
            let slot = self.get_slot(i);
            let offset = slot.get_offset();
            let size = slot.get_size();

            if IS_LEAF {
                let value_size = slot.get_value_size();
                let size = slot.get_size();
                let offset = slot.get_offset();
                let (jk, jv) = if slot.is_infinite() {
                    (JsonValue::Null, JsonValue::Null)
                } else {
                    //trace!("get key value [{} {}], [{} {}]",
                    // offset, size - value_size, offset + size - value_size, value_size);
                    let key = self.get_key(i);
                    let value = self.get_value(i);

                    let jk = context.to_json(key)?;
                    let jv = value2json.to_json(value)?;
                    (jk, jv)
                };

                let _ = slot_array.push(object! {
                "offset": offset,
                "size":size,
                "value_size":value_size});
                let _ = key_value_array.push(object! {
                    "key" : jk,
                    "value" : jv,
                });
            } else {
                let id = slot.get_page_id();
                let _ = slot_array.push(object! {
                "offset": offset,
                "size":size,
                "page_id":id});

                let jk = if slot.is_infinite() {
                    JsonValue::Null
                } else {
                    let key = self.get_key(i);
                    let jk = context.to_json(key)?;
                    jk
                };
                let _ = key_value_array.push(object! {
                    "key" : jk,
                    "id" : id,
                });
            }
        }
        let mut json_obj = self.page_meta_to_json()?;
        let r1 = json_obj.insert("slot", slot_array);
        match r1 {
            Err(e) => {
                return Err(ET::JSONError(e.to_string()));
            }
            _ => {}
        }
        let r2 = json_obj.insert("key_value", key_value_array);
        match r2 {
            Err(e) => {
                return Err(ET::JSONError(e.to_string()));
            }
            _ => {}
        }
        Ok(json_obj)
    }

    pub fn check_child_le_parent<C: Compare<[u8]>, const PARENT_IS_LEAF: bool>(
        &self,
        parent_page: &PageBTree<PARENT_IS_LEAF>,
        cmp: &C,
    ) {
        assert!(!PARENT_IS_LEAF);
        let child_high_key = self.get_last_key();
        let parent_high_key = parent_page.get_last_key();
        match (child_high_key, parent_high_key) {
            (Some(c), Some(p)) => {
                let ord = bt_key_cmp_empty_as_max(cmp, c, p);
                assert!(ord.is_le());
            }
            _ => {}
        }
    }

    pub fn check_left_lt_right<C: Compare<[u8]>, KT: ToJson, VT: ToJson>(
        &self,
        right_page: &Self,
        cmp: &C,
        k2j: &KT,
        v2j: &VT,
    ) {
        let right_low_key = right_page.get_first_key();
        let left_high_key = self.get_last_key();

        match (left_high_key, right_low_key) {
            (Some(l), Some(h)) => {
                let ord = bt_key_cmp_empty_as_max(cmp, l, h);
                if !ord.is_lt() {
                    error!("left page :{}", self.to_json(k2j, v2j).unwrap().to_string());
                    error!(
                        "right page :{}",
                        right_page.to_json(k2j, v2j).unwrap().to_string()
                    );
                    error!("left high key :{}", k2j.to_json(l).unwrap().to_string());
                    error!("right low key :{}", k2j.to_json(h).unwrap().to_string());
                }
                assert!(ord.is_lt());
            }
            _ => {}
        }
        assert_eq!(self.get_right_id(), right_page.get_id());
        assert_eq!(self.get_id(), right_page.get_left_id());
        assert_eq!(self.get_page_level(), right_page.get_page_level());
    }

    pub fn check_invariant<C: Compare<[u8]>>(&self, cmp: &C) {
        self.check_page_invariant(cmp);
    }

    pub fn check_high_key(&self) {
        match self.opt_high_key() {
            Some(k) => {
                if k.is_empty() {
                    assert_eq!(self.get_right_id(), INVALID_PAGE_ID);
                } else {
                    assert_ne!(self.get_right_id(), INVALID_PAGE_ID);
                }
            }
            None => {}
        }
    }

    pub fn check_page_invariant<C: Compare<[u8]>>(&self, cmp: &C) {
        assert!(self.get_slot_end() <= self.get_data_begin());
        let count = self.get_count();
        let is_leaf = self.is_leaf();
        assert_eq!(is_leaf, IS_LEAF);
        for i in 0..count {
            if i + 1 < count {
                let (k1, k2) = if IS_LEAF {
                    let k1 = self.get_key(i);
                    let k2 = self.get_key(i + 1);
                    (k1, k2)
                } else {
                    let s1 = self.get_slot(i);
                    let s2 = self.get_slot(i + 1);
                    assert_ne!(s1.get_page_id(), s2.get_page_id());
                    let k1 = self.get_key(i);
                    let k2 = self.get_key(i + 1);

                    (k1, k2)
                };

                let ord = bt_key_cmp_empty_as_max(cmp, k1, k2);
                assert!(ord.is_lt());
            }
        }
    }
}

impl<'a, const IS_LEAF: bool> PageBTreeMut<'a, IS_LEAF> {
    pub fn new(vec: &'a mut Vec<u8>) -> Self {
        // no assert
        Self { vec }
    }

    pub fn from(vec: &'a mut Vec<u8>) -> Self {
        assert_eq!(is_leaf_page(vec), IS_LEAF);
        Self::new(vec)
    }

    pub fn to_json<KT: ToJson, VT: ToJson>(&self, k2j: &KT, v2j: &VT) -> Result<JsonValue, ET> {
        PageBTree::<IS_LEAF>::from(self.vec).to_json(k2j, v2j)
    }
    pub fn as_immutable(&self) -> PageBTree<IS_LEAF> {
        PageBTree::from(self.vec)
    }

    pub fn page_id_of(guard: &'a mut Vec<u8>) -> PageId {
        let p = Self::from(guard);
        p.get_id()
    }

    pub fn is_leaf(&self) -> bool {
        self.as_immutable().is_leaf()
    }

    pub fn get_high_key_vec(&self) -> Option<Vec<u8>> {
        self.as_immutable().get_high_key_vec()
    }

    pub fn contain<C, K>(&self, range: &RangeBounds<K>, cmp: &C) -> bool
    where
        C: Compare<[u8]>,
        K: Slice,
    {
        self.as_immutable().contain(range, cmp)
    }

    pub fn get_id(&self) -> PageId {
        PageHdr::from(self.vec).get_id()
    }

    pub fn format(&mut self, page_id: PageId, left_id: PageId, right_id: PageId, level: u32) {
        let mut header = PageHdrMut::from(self.vec);
        header.format(page_id, left_id, right_id, FileKind::FileBTree);
        self.set_count(0);
        self.set_active_count(0);
        self.set_data_begin(self.vec.len() as u32);
        self.set_page_level(level);
    }

    pub fn copy_from(&mut self, other: &Self) {
        if self.vec.len() != other.vec.len() {
            panic!("cannot happen")
        }
        unsafe {
            std::ptr::copy(other.vec.as_ptr(), self.vec.as_mut_ptr(), self.vec.len());
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn set_high_key_infinite(&mut self) -> bool {
        let count = self.get_count();
        let p1 = self.get_slot_end();
        let p2 = self.get_data_begin();
        if p1 + SlotBt::<IS_LEAF>::slot_size() <= p2 {
            let mut slot = self.get_slot_mut(count);
            slot.set_infinite();
            self.set_count(count + 1);
            self.set_active_count(count + 1);
            assert!(self.get_slot_end() <= self.get_data_begin());
            true
        } else {
            false
        }
    }

    pub fn set_id(&mut self, page_id: PageId) {
        PageHdrMut::from(self.vec).set_id(page_id);
    }

    pub fn get_right_id(&self) -> PageId {
        self.as_immutable().get_right_id()
    }

    pub fn set_right_id(&mut self, page_id: PageId) {
        PageHdrMut::from(self.vec).set_right_id(page_id);
    }

    pub fn get_left_id(&self) -> PageId {
        self.as_immutable().get_left_id()
    }

    pub fn set_left_id(&mut self, page_id: PageId) {
        PageHdrMut::from(self.vec).set_left_id(page_id);
    }

    pub fn get_count(&self) -> u32 {
        self.as_immutable().get_count()
    }

    pub fn increase_seq_no(&mut self) {
        PageHdrMut::from(&mut self.vec).increase_seq_no();
    }

    pub fn set_count(&mut self, count: u32) {
        PageHdrBtMut::from(self.vec).set_count(count);
    }

    pub fn get_type(&self) -> FileKind {
        self.as_immutable().get_type()
    }

    pub fn set_type(&mut self, page_type: u32) {
        PageHdrMut::from(self.vec).set_type(page_type)
    }

    pub fn get_active_count(&self) -> u32 {
        self.as_immutable().get_active_count()
    }

    pub fn set_active_count(&mut self, active_count: u32) {
        PageHdrBtMut::from(self.vec).set_active_count(active_count);
    }

    pub fn get_slot_end(&self) -> u32 {
        self.as_immutable().get_slot_end()
    }

    pub fn get_data_begin(&self) -> u32 {
        self.as_immutable().get_data_begin()
    }

    pub fn set_data_begin(&mut self, data_start: u32) {
        PageHdrBtMut::from(self.vec).set_data_begin(data_start);
    }

    pub fn get_page_size(&self) -> u32 {
        self.as_immutable().get_page_size()
    }

    pub fn set_page_size(&mut self, page_size: u32) {
        PageHdrMut::from(self.vec).set_size(page_size);
    }

    pub fn get_page_level(&self) -> u32 {
        self.as_immutable().get_page_level()
    }

    pub fn set_page_level(&mut self, page_level: u32) {
        PageHdrBtMut::from(self.vec).set_page_level(page_level);
    }

    pub fn get_free_space(&self) -> u32 {
        self.as_immutable().get_free_space()
    }

    pub fn get_slot(&self, index: u32) -> SlotBt<IS_LEAF> {
        page_bt_get_slot(self.vec, index)
    }

    pub fn slot_offset(index: u32) -> u32 {
        let offset = index * SlotBt::<IS_LEAF>::slot_size() + BT_PAGE_HEADER_SIZE;
        offset
    }

    pub fn get_slot_slice_mut(page: &mut [u8], index: u32) -> &mut [u8] {
        let offset = Self::slot_offset(index) as usize;
        let slice = &mut page[offset..offset + SlotBt::<IS_LEAF>::slot_size() as usize];
        return slice;
    }

    pub fn get_slot_mut(&mut self, index: u32) -> SlotBtMut<IS_LEAF> {
        let s = unsafe { std::slice::from_raw_parts_mut(self.vec.as_mut_ptr(), self.vec.len()) };
        let slice = Self::get_slot_slice_mut(s, index);
        SlotBtMut::new(slice)
    }

    pub fn get_key(&self, index: u32) -> &[u8] {
        page_bt_get_key::<IS_LEAF>(self.vec, index)
    }

    pub fn get_value(&self, index: u32) -> &[u8] {
        page_bt_get_value::<IS_LEAF>(self.vec, index)
    }

    fn get_payload(&self, offset: u32, size: u32) -> &[u8] {
        page_bt_get_slice(self.vec, offset, size)
    }

    fn get_slice_mut(&mut self, offset: u32, size: u32) -> &mut [u8] {
        let slice = unsafe {
            let s = self.vec.as_mut_ptr().add(offset as usize);
            std::slice::from_raw_parts_mut(s, size as usize)
        };
        slice
    }

    /// Returns LBResult
    /// Find the first element greater or equal than ${k}
    #[cfg_attr(debug_assertions, inline(never))]
    pub fn lower_bound<K, C: Compare<[u8]>>(&self, key: &K, cmp: &C) -> LBResult
    where
        K: Slice,
    {
        self.as_immutable().lower_bound(key, cmp)
    }

    pub fn remove(&mut self, index: u32, modify_list: &mut ModifyList) {
        let count = self.get_count();
        assert!(index < count);
        let opt_op = self.opt_high_key_for_delete(index);
        match opt_op {
            None => {}
            Some((op, old, new)) => {
                modify_list.append_trace(format!("rm k {}", self.get_id()));
                modify_list.add_operation(self.get_id(), op, old, new);
            }
        }
        let slot_size = SlotBt::<IS_LEAF>::slot_size();
        let s2 = BT_PAGE_HEADER_SIZE + (index + 1) * slot_size;
        let e2 = BT_PAGE_HEADER_SIZE + count * slot_size;
        self.mem_move_left(s2, e2, slot_size);
        self.set_count(count - 1);
        self.set_active_count(count - 1);

        assert!(self.get_slot_end() <= self.get_data_begin());
    }

    fn put_key_value(
        &mut self,
        slot_offset: u32,
        data_offset: u32,
        cap: u32,
        size: u32,
        key: &[u8],
        value: &[u8],
    ) {
        let s = unsafe {
            let s = self.vec.as_mut_ptr().add(slot_offset as usize);
            std::slice::from_raw_parts_mut(s, SlotBt::<LEAF_PAGE>::slot_size() as usize)
        };
        let mut slot = SlotBtMut::<LEAF_PAGE>::new(s);
        slot.set_capacity(cap);
        slot.set_size(size);
        slot.set_value_size(value.len() as u32);
        slot.set_offset(data_offset as u32);

        unsafe {
            let off_key = data_offset as usize;
            let key_len = key.len() as usize;
            let off_value = (data_offset as usize + key.len()) as usize;
            let value_len = value.len() as usize;
            // trace!("insert key value [{} {}] [{} {}]", off_key, key_len, off_value, value_len);
            // copy key
            std::ptr::copy(key.as_ptr(), self.vec.as_mut_ptr().add(off_key), key_len);

            // copy value
            std::ptr::copy(
                value.as_ptr(),
                self.vec.as_mut_ptr().add(off_value),
                value_len,
            );
        }
    }

    fn put_key(
        &mut self,
        slot_offset: u32,
        data_offset: u32,
        cap: u32,
        size: u32,
        key: &[u8],
        page_id: PageId,
    ) {
        let s = unsafe {
            let s = self.vec.as_mut_ptr().add(slot_offset as usize);
            std::slice::from_raw_parts_mut(s, SlotBt::<IS_LEAF>::slot_size() as usize)
        };
        let mut slot = SlotBtMut::<IS_LEAF>::new(s);
        slot.set_capacity(cap);
        slot.set_size(size);
        slot.set_page_id(page_id);
        slot.set_offset(data_offset as u32);
        unsafe {
            // copy key
            std::ptr::copy(
                key.as_ptr(),
                self.vec.as_mut_ptr().add(data_offset as usize),
                key.len() as usize,
            );
        }
    }

    fn make_slot_space(&mut self, offset_slot_min: u32, offset_slot_max: u32, new_data_start: u32) {
        if offset_slot_min < offset_slot_max {
            self.mem_move_right(
                offset_slot_min,
                offset_slot_max,
                SlotBt::<IS_LEAF>::slot_size(),
            );
        } else {
            assert_eq!(offset_slot_min, offset_slot_max);
            // trace!("min,max: {}{}", offset_slot_min, offset_slot_max);
        }
        let new_count = self.get_count() + 1;
        let new_active_count = self.get_active_count() + 1;

        self.set_count(new_count);
        self.set_active_count(new_active_count);
        self.set_data_begin(new_data_start);
        assert!(self.get_slot_end() <= self.get_data_begin());
    }

    pub fn opt_high_key(&self) -> Option<Vec<u8>> {
        self.as_immutable().opt_high_key()
    }

    // move left N size position, start at P, end at E
    // EX: N=3
    //      |-------S*********E******|
    //      |----S*********E---******|
    fn mem_move_left(&mut self, start: u32, end: u32, size: u32) {
        unsafe {
            let s = start as usize;
            let e = end as usize;
            let n = size as usize;
            std::ptr::copy(
                self.vec.as_ptr().add(s),
                self.vec.as_mut_ptr().add(s - n),
                e - s,
            );
        }
    }

    // move right N size position, start at P, end at E
    // EX: N=3
    //      |----S*********E---******|
    //      |-------S*********E******|
    fn mem_move_right(&mut self, start: u32, end: u32, size: u32) {
        unsafe {
            let s = start as usize;
            let e = end as usize;
            let n = size as usize;
            std::ptr::copy(
                self.vec.as_ptr().add(s),
                self.vec.as_mut_ptr().add(s + n),
                e - s,
            );
        }
    }

    fn mem_copy(&mut self, offset: u32, slice: &[u8]) {
        unsafe {
            std::ptr::copy(
                slice.as_ptr(),
                self.vec.as_mut_ptr().add(offset as usize),
                slice.len(),
            );
        }
    }

    fn opt_high_key_for_update<K: Slice>(
        &self,
        index: u32,
        key: &K,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        self.as_immutable().opt_high_key_for_update(index, key)
    }

    fn opt_high_key_for_delete(
        &self,
        index: u32,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        self.as_immutable().opt_high_key_for_delete(index)
    }

    fn opt_high_key_for_insert<K: Slice>(
        &self,
        index: u32,
        key: &K,
    ) -> Option<(ParentModifyOp, Option<Vec<u8>>, Option<Vec<u8>>)> {
        self.as_immutable().opt_high_key_for_insert(index, key)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn leaf_update_index<K, V>(
        &mut self,
        index: u32,
        key: &K,
        value: &V,
        modify_list: &mut ModifyList,
    ) -> bool
    where
        K: Slice,
        V: Slice,
    {
        let ks = key.as_slice();
        let vs = value.as_slice();
        let offset_slot_max = self.get_slot_end();
        let data_start = self.get_data_begin();
        let slot = self.get_slot(index);
        let slot_offset = slot.get_offset();
        let size = slot.get_size();
        let old_cap = slot.get_capacity();
        let value_size = slot.get_value_size();
        let new_size = (ks.len() + vs.len()) as u32;
        let new_cap = capacity_of(new_size);
        // new key value size - old key value size > available space
        if offset_slot_max + new_cap > data_start + old_cap {
            return false;
        }

        if new_cap > old_cap && offset_slot_max + new_cap > data_start {
            return false;
        }
        let opt_op = self.opt_high_key_for_update(index, key);
        match opt_op {
            Some((op, old, new)) => {
                modify_list.append_trace(format!("l u  {}", self.get_id()));
                modify_list.add_operation(self.get_id(), op, old, new);
            }
            None => {}
        }

        let new_offset = if new_cap > old_cap {
            // not enough space in old location
            let offset = data_start - new_cap;
            self.set_data_begin(offset);
            let mut slot_mut = self.get_slot_mut(index);
            slot_mut.set_capacity(new_cap);
            offset
        } else {
            slot_offset
        };
        assert_eq!(ks.len() as u32, size - value_size);

        self.mem_copy(new_offset, ks);
        self.mem_copy(new_offset + ks.len() as u32, vs);

        let mut slot_mut = self.get_slot_mut(index);
        slot_mut.set_size(new_size);
        slot_mut.set_offset(new_offset);
        slot_mut.set_value_size(vs.len() as u32);

        true
    }

    // leaf page update or insert a key value pair to a index position
    pub fn leaf_upsert_key_value<K, V>(
        &mut self,
        index: u32,
        upsert: Upsert,
        key: &K,
        value: &V,
        modify_list: &mut ModifyList,
        out: &mut Option<TxOption>,
    ) -> bool
    where
        K: Slice,
        V: Slice,
    {
        assert!(self.is_leaf());
        let ok = match upsert {
            Upsert::Update => self.leaf_update_index(index, key, value, modify_list),
            Upsert::Insert => self.leaf_insert_index(index, key, value, modify_list),
        };
        if ok {
            match out {
                None => {}
                Some(opt) => {
                    opt.add_set_key(Vec::from(key.as_slice()), self.get_id());
                    let mut slot = self.get_slot_mut(index);
                    slot.set_write_flag();
                }
            }
        }
        ok
    }

    #[cfg_attr(debug_assertions, inline(never))]
    /// return
    ///     UpsertOk
    ///     NeedSplit...
    pub fn leaf_search_upsert<K, V, C: Compare<[u8]>>(
        &mut self,
        key: &K,
        value: &V,
        upsert: Upsert,
        right_link_id: PageId,
        cmp: &C,
        modify_list: &mut ModifyList,
        out: &mut Option<TxOption>,
    ) -> Result<ResultUpsert, ET>
    where
        K: Slice,
        V: Slice,
    {
        let result = self.lower_bound::<K, C>(key, cmp);
        let index = match result {
            LBResult::Found(index, equal) => {
                match upsert {
                    Upsert::Update => {
                        if !equal {
                            return Ok(ResultUpsert::NonExistingKey);
                        }
                    }
                    Upsert::Insert => {
                        if equal {
                            return Ok(ResultUpsert::ExistingKey);
                        }
                    }
                }
                index
            }
            LBResult::NotFound => {
                let right_id = self.get_right_id();
                if right_id != right_link_id {
                    if upsert == Upsert::Update {
                        return Ok(ResultUpsert::NonExistingKey);
                    } else {
                        self.get_count()
                    }
                } else {
                    return Ok(ResultUpsert::SearchRight(right_id));
                }
            }
        };
        let ok = self.leaf_upsert_key_value(index, upsert, key, value, modify_list, out);
        if ok {
            Ok(ResultUpsert::UpsertOk)
        } else {
            Ok(ResultUpsert::NeedSplit(index))
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn leaf_insert_index<K, V>(
        &mut self,
        index: u32,
        key: &K,
        value: &V,
        modify_list: &mut ModifyList,
    ) -> bool
    where
        K: Slice,
        V: Slice,
    {
        let ks = key.as_slice();
        let vs = value.as_slice();
        //const_assert!(LEAF);
        let slot_end = self.get_slot_end();
        let data_start = self.get_data_begin();
        let size = (ks.len() + vs.len()) as u32;
        let cap = capacity_of(size);
        if slot_end + SlotBt::<IS_LEAF>::slot_size() as u32 + cap > data_start {
            return false;
        }
        let opt_high = self.opt_high_key_for_insert(index, key);
        match opt_high {
            Some((op, old, new)) => {
                modify_list.append_trace(format!("l i  {}", self.get_id()));
                modify_list.add_operation(self.get_id(), op, old, new);
            }
            None => {}
        }
        let slot_insert_offset =
            index * SlotBt::<IS_LEAF>::slot_size() as u32 + BT_PAGE_HEADER_SIZE as u32;

        self.make_slot_space(slot_insert_offset, slot_end, data_start - cap);
        self.put_key_value(slot_insert_offset, data_start - cap, cap, size, ks, vs);
        true
    }

    #[cfg_attr(debug_assertions, inline(never))]
    // optional update high key
    pub fn non_leaf_update_index<K: Slice>(
        &mut self,
        index: u32,
        key: &K,
        page_id: PageId,
        modify_list: &mut ModifyList,
    ) -> bool {
        assert_ne!(page_id, self.get_id());
        let ks = key.as_slice();
        // trace!("page:{}, inf:{}", self.get_page_id(), page_id);
        if ks.is_empty() {
            let opt_op = self.opt_high_key_for_update(index, key);
            match opt_op {
                Some((op, old, new)) => {
                    modify_list.append_trace(format!("nl u {}", self.get_id()));

                    modify_list.add_operation(self.get_id(), op, old, new);
                }
                None => {}
            }
            let mut slot = self.get_slot_mut(index);
            slot.set_infinite();
            slot.set_page_id(page_id);
            true
        } else {
            assert!(index < self.get_count());
            let slot_end = self.get_slot_end();
            let data_beg = self.get_data_begin();
            let slot = self.get_slot(index);
            let page_id = slot.get_page_id();
            let old_capacity = slot.get_capacity();
            let old_key_off = slot.get_offset();
            let key_size = ks.len() as u32;
            let new_capacity = capacity_of(key_size);
            // OK for empty_end - empty_beg + old_key_size - key_size >= 0
            if data_beg + old_capacity < slot_end + new_capacity {
                return false;
            }
            let opt_op = self.opt_high_key_for_update(index, key);
            match opt_op {
                Some((op, old, new)) => {
                    modify_list.append_trace(format!("nl u{}", self.get_id()));
                    modify_list.add_operation(self.get_id(), op, old, new);
                }
                None => {}
            }
            let mut slot_mut = self.get_slot_mut(index);
            if new_capacity <= old_capacity {
                slot_mut.set_page_id(page_id);
                slot_mut.set_size(key_size);
                self.mem_copy(old_key_off, ks);
            } else {
                // old slot [old_offset, old_capacity]
                let new_data_beg = data_beg + old_capacity - new_capacity;
                slot_mut.set_capacity(new_capacity);
                slot_mut.set_size(key_size);
                slot_mut.set_page_id(page_id);
                slot_mut.set_offset(new_data_beg);
                if old_capacity != 0 && old_key_off > data_beg {
                    // not a infinite value
                    self.mem_move_right(data_beg, old_key_off, old_capacity);
                }
                self.set_data_begin(new_data_beg);
                self.mem_copy(new_data_beg, ks);
                assert!(self.get_slot_end() <= self.get_data_begin());
            }

            return true;
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    /// return ResultUpsert
    /// NeedSplit, or UpsertOk
    pub fn non_leaf_search_and_update_key<K: Slice, K2: Slice, C: Compare<[u8]>>(
        &mut self,
        key: &K,
        old_key: &K2,
        page_id: PageId,
        cmp: &C,
        modify_list: &mut ModifyList,
    ) -> Result<ResultUpsert, ET> {
        let result = self.lower_bound::<_, C>(old_key, cmp);
        let mut index = match result {
            LBResult::Found(index, equal) => {
                if !equal {
                    return Ok(ResultUpsert::NonExistingKey);
                }
                index
            }
            LBResult::NotFound => {
                let next_page_id = self.get_right_id();
                return if next_page_id == INVALID_PAGE_ID {
                    // possible when spliting
                    Ok(ResultUpsert::NonExistingKey)
                } else {
                    Ok(ResultUpsert::SearchRight(next_page_id))
                };
            }
        };
        let count = self.get_count();
        let slot = self.get_slot(index);
        let mut this_page_id = slot.get_page_id();
        while index < count && this_page_id != page_id {
            let slot = self.get_slot(index);
            this_page_id = slot.get_page_id();
            index += 1;
        }

        if index == count {
            return Ok(ResultUpsert::NonExistingKey);
        }
        if index > 1 {
            let prev = self.get_key(index - 1);
            let order = bt_key_cmp_empty_as_max(cmp, prev, key.as_slice());
            if !order.is_lt() {
                return Err(ET::PageUpdateErrorHighKeySlot);
            }
        }
        if index + 1 < count {
            let next = self.get_key(index + 1);
            let order = bt_key_cmp_empty_as_max(cmp, key.as_slice(), next);
            if !order.is_lt() {
                //trace!("index {} old key {}, key: {}",
                //    index,
                //    old_key.to_json(desc).unwrap().to_string(),
                //    key.to_json(desc).unwrap().to_string());
                return Err(ET::PageUpdateErrorHighKeySlot);
            }

            assert!(order.is_lt())
        } else if index + 1 == count {
            // is the last one
            let order = bt_key_cmp_empty_as_max(cmp, old_key.as_slice(), key.as_slice());
            if order.is_lt() {
                // old key is lower than new key, and this key is the last key
                // we could not test the updating key is lower than next key
                // so, we return error to let the caller know
                return Err(ET::PageUpdateErrorHighKeySlot);
            }
        } else {
            // we could not test the updating key is lower than next key
            // so, we return error to let the caller know
            return Err(ET::PageUpdateErrorHighKeySlot);
        }
        let ok = self.non_leaf_update_index(index, key, page_id, modify_list);
        if ok {
            Ok(ResultUpsert::UpsertOk)
        } else {
            Ok(ResultUpsert::NeedSplit(index))
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    /// return ResultUpsert
    /// NeedSplit, or UpsertOk
    pub fn non_leaf_search_and_insert_child<K: Slice, K2: Slice, C: Compare<[u8]>>(
        &mut self,
        key: &K,
        high_key: &K2,
        page_id: PageId,
        right_boundary_page_id: PageId,
        cmp: &C,
        modify_list: &mut ModifyList,
    ) -> Result<ResultUpsert, ET> {
        let result = self.lower_bound::<K, C>(key, cmp);
        // trace!("non leaf insert key: {}, {}", page_id, key.to_json(&self.key_desc).unwrap().to_string());
        // trace!("non leaf insert key: {}", bt_page.to_json(&self.key_desc, &self.value_desc).unwrap().to_string());
        let index = match result {
            LBResult::Found(index, equal) => {
                if equal {
                    return Ok(ResultUpsert::ExistingKey);
                }
                index
            }
            LBResult::NotFound => {
                // test existing next page id
                let next_page_id = self.get_right_id();
                let index = if next_page_id == right_boundary_page_id {
                    // no right link pointer, insert at the last position
                    self.get_count()
                } else {
                    // trace!("non leaf insert key: {}, {}", page_id, key.to_json(&self.key_desc).unwrap().to_string());
                    // trace!("non leaf insert key: {}", bt_page.to_json(&self.key_desc, &self.value_desc).unwrap().to_string());
                    assert_ne!(next_page_id, INVALID_PAGE_ID);
                    return Ok(ResultUpsert::SearchRight(next_page_id));
                };
                // <= parent keys

                let ord = bt_key_cmp_empty_as_max(cmp, key.as_slice(), high_key.as_slice());
                match ord {
                    Ordering::Less | Ordering::Equal => {}
                    _ => {
                        //trace!("page: {}", self.to_json(desc, value_desc).unwrap().to_string());
                        //trace!("non leaf insert id {}, key:{}, parent high key:{}",
                        //    page_id,
                        //    key.to_json(desc).unwrap().to_string(),
                        //    high_key.to_json(desc).unwrap().to_string());
                        //assert!(false);
                    }
                }
                index
            }
        };
        let ok = self.non_leaf_insert_index(index, key, page_id, modify_list);
        if ok {
            Ok(ResultUpsert::UpsertOk)
        } else {
            Ok(ResultUpsert::NeedSplit(index))
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn non_leaf_insert_index<K: Slice>(
        &mut self,
        index: u32,
        key: &K,
        page_id: PageId,
        modify_list: &mut ModifyList,
    ) -> bool {
        let ks = key.as_slice();
        assert_ne!(page_id, self.get_id());
        assert_ne!(page_id, INVALID_PAGE_ID);
        // is infinite high key
        let offset_slot_max = self.get_slot_end();
        let size = ks.len() as u32;
        let cap = capacity_of(size);
        let data_start = self.get_data_begin();
        if offset_slot_max + SlotBt::<NON_LEAF_PAGE>::slot_size() as u32 + cap > data_start {
            return false;
        }
        let opt_high = self.opt_high_key_for_insert(index, key);
        match opt_high {
            Some((op, old, new)) => {
                modify_list.append_trace(format!("nl i {}", self.get_id()));
                modify_list.add_operation(self.get_id(), op, old, new);
            }
            None => {}
        }

        if ks.is_empty() {
            let mut slot = self.get_slot_mut(index);
            slot.set_infinite();
            slot.set_page_id(page_id);
            self.set_count(self.get_count() + 1);
            self.set_active_count(self.get_active_count() + 1);
            assert!(self.get_slot_end() <= self.get_data_begin());
        } else {
            let offset_slot_min =
                index * SlotBt::<IS_LEAF>::slot_size() as u32 + BT_PAGE_HEADER_SIZE as u32;
            self.make_slot_space(offset_slot_min, offset_slot_max, data_start - cap);
            self.put_key(offset_slot_min, data_start - cap, cap, size, ks, page_id);
        };
        true
    }

    pub fn split_position<const LEAF: bool>(
        &self,
        new_add_index: u32,
        new_add_size: u32,
    ) -> Result<(u32, Option<(u32, bool)>), ET> {
        self.as_immutable()
            .split_position::<LEAF>(new_add_index, new_add_size)
    }

    pub fn split_page(
        &mut self,
        page: &mut PageBTreeMut<'a, IS_LEAF>,
        split_position: u32,
        modify_list: &mut ModifyList,
    ) {
        let left_old_high_key = self.get_high_key_vec();
        self.split(page.vec, split_position);
        let left_new_high_key = self.get_high_key_vec();
        let right_high_key = page.get_high_key_vec();

        match (left_old_high_key, left_new_high_key) {
            (Some(old_k), Some(new_k)) => {
                modify_list.append_trace(format!("sp u {}", self.get_id()));
                modify_list.add_operation(
                    self.get_id(),
                    ParentModifyOp::ParentUpdateSub,
                    Some(old_k),
                    Some(new_k),
                )
            }
            (Some(old_k), None) => {
                modify_list.append_trace(format!("s del {}", self.get_id()));
                modify_list.add_operation(
                    self.get_id(),
                    ParentModifyOp::ParentDeleteSub,
                    Some(old_k),
                    None,
                );
            }
            _ => {
                panic!("error!");
            }
        }
        // add new page first, ahead of add insert operations
        modify_list.new_page(page.get_id());
        match right_high_key {
            Some(k) => {
                modify_list.append_trace(format!("sp i {}", self.get_id()));
                modify_list.add_operation(
                    page.get_id(),
                    ParentModifyOp::ParentInsertSub,
                    None,
                    Some(k),
                );
            }
            None => {
                // no high key, impossible
                panic!("error")
            }
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub fn split(&mut self, vec: &mut Vec<u8>, split_position: u32) {
        let max = self.get_count();
        assert!(split_position <= max);
        let mut index = split_position;
        let mut r_slot_end = BT_PAGE_HEADER_SIZE;
        let mut r_data_beg = self.get_page_size();
        while index < max {
            let slot = self.get_slot(index);
            let mut page = PageBTreeMut::<IS_LEAF>::from(vec);
            if slot.is_infinite() {
                let slice = page.get_slice_mut(r_slot_end, SlotBt::<IS_LEAF>::slot_size());
                let mut right_slot = SlotBtMut::<IS_LEAF>::new(slice);
                right_slot.set_infinite();
                right_slot.set_page_id(slot.get_page_id());
            } else {
                let offset = slot.get_offset();
                let payload = self.get_payload(offset, slot.get_size());
                let payload_size = slot.get_size();
                let cap = slot.get_capacity();
                assert_eq!(payload_size, payload.len() as u32);
                assert!(r_slot_end + cap < r_data_beg);
                r_data_beg = r_data_beg - cap;
                if IS_LEAF {
                    let value_size = slot.get_value_size();
                    let key_size = payload_size - value_size;
                    let key = BtKeyRaw::from_slice(&payload[0..key_size as usize]);
                    let value =
                        TupleRaw::from_slice(&payload[key_size as usize..payload_size as usize]);
                    page.put_key_value(
                        r_slot_end,
                        r_data_beg,
                        cap,
                        payload_size,
                        key.slice(),
                        value.slice(),
                    );
                } else {
                    let page_id = slot.get_page_id();
                    page.put_key(r_slot_end, r_data_beg, cap, payload_size, payload, page_id);
                }
            }
            r_slot_end = r_slot_end + SlotBt::<IS_LEAF>::slot_size();
            index += 1;
        }
        let remove_cnt = max - split_position;
        let mut new_page = PageBTreeMut::<IS_LEAF>::from(vec);
        new_page.set_data_begin(r_data_beg as u32);
        new_page.set_count(remove_cnt);
        new_page.set_active_count(remove_cnt);
        new_page.set_page_level(self.get_page_level());
        new_page.set_right_id(self.get_right_id());
        new_page.set_left_id(self.get_id());
        assert!(new_page.get_slot_end() <= new_page.get_data_begin());

        let page_size = self.get_page_size();
        let l_data_beg = self.get_data_begin();
        let r_data_size = page_size - r_data_beg;

        self.set_count(split_position);
        self.set_data_begin(l_data_beg + r_data_size);
        self.set_active_count(self.get_active_count() - remove_cnt);
        self.set_right_id(new_page.get_id());
        self.increase_seq_no();
        assert!(self.get_slot_end() <= self.get_data_begin());
    }

    /// return value
    /// (
    ///     u32,                split position
    ///     Option<u32, bool>   optional insert position for newly insert/update row,
    ///                         position, and
    ///                         would go to left node
    /// )
    #[cfg_attr(debug_assertions, inline(never))]
    pub fn re_organize_and_split_position<const LEAF: bool>(
        &mut self,
        index: u32,
        size: u32,
    ) -> Result<(u32, Option<(u32, bool)>), ET> {
        self.re_organize::<LEAF>();
        let (split_pos, opt_index) = self.split_position::<LEAF>(index, size)?;

        Ok((split_pos, opt_index))
    }

    pub fn re_organize<const LEAF: bool>(&mut self) {
        let page_size = self.get_page_size();
        let vec_size = page_size - self.get_data_begin();

        let mut vec: Vec<u8> = Vec::new();
        vec.resize(vec_size as usize, 0);
        let count = self.get_count();
        let mut length: u32 = 0;
        for i in 0..count {
            let (offset, size, cap, is_inf) = {
                let s = self.get_slot(i);
                let size = s.get_size();
                let cap = capacity_of(size);
                assert!(cap <= s.get_capacity());
                (
                    s.get_offset(),
                    s.get_size(),
                    s.get_capacity(),
                    s.is_infinite(),
                )
            };

            if size > 0 {
                let payload = self.get_payload(offset, size);
                let end = vec.len() - length as usize;
                let begin = vec.len() - length as usize - cap as usize;
                unsafe {
                    std::ptr::copy(
                        payload.as_ptr(),
                        vec.as_mut_slice()[begin..end].as_mut_ptr(),
                        size as usize,
                    )
                }
                length += cap as u32;
            }
            if is_inf {
                let mut slot = self.get_slot_mut(i);
                let page_id = slot.get_page_id();
                slot.set_infinite();
                slot.set_page_id(page_id);
            } else {
                let mut slot = self.get_slot_mut(i);
                slot.set_capacity(cap);
                slot.set_size(size);
                slot.set_offset(page_size - length as u32);
            }
        }

        unsafe {
            let dest_off = page_size - length;
            let src_begin = vec.len() - length as usize;
            std::ptr::copy(
                vec.as_ptr().add(src_begin),
                self.vec.as_mut_ptr().add(dest_off as usize),
                length as usize,
            )
        }

        self.set_data_begin(page_size - length);
        assert!(self.get_slot_end() <= self.get_data_begin());
    }
}
