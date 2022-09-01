use std::cmp::Ordering;

use common::result;

use crate::d_type::data_type_func::{DataTypeInit, DatumFunc};
use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;

#[derive(Clone)]
pub struct DatumOper {
    item_value: ItemValue,
    is_fixed_length: bool,
    offset: u32,
    max_length: u32,
    fun_datum: DatumFunc,
}

impl DatumOper {
    pub fn from(
        dt: &DataTypeInit,
        is_fixed_length: bool,
        max_length: u32,
        offset: u32,
    ) -> DatumOper {
        DatumOper {
            item_value: dt.item_value_default.clone(),
            is_fixed_length,
            offset,
            max_length,
            fun_datum: dt.fn_datum,
        }
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
    pub fn is_fixed_length(&self) -> bool {
        self.is_fixed_length
    }

    pub fn max_length(&self) -> u32 {
        self.max_length
    }

    pub fn set_max_length(&mut self, max_length: u32) {
        self.max_length = max_length;
    }

    pub fn set_fixed_length(&mut self, fixed_length: bool) {
        self.is_fixed_length = fixed_length;
    }

    pub fn set_offset(&mut self, offset: u32) {
        self.offset = offset;
    }
    pub fn datum_compare(&self, s1: &[u8], s2: &[u8]) -> Ordering {
        (self.fun_datum.fn_compare)(s1, s2)
    }

    pub fn datum_to_string(&self, slice: &[u8]) -> result::Result<String> {
        let mut item = self.item_value.clone();
        item.init_from_slice(slice)?;
        item.to_str()
    }

    pub fn datum_to_item(&self, slice: &[u8]) -> result::Result<ItemValue> {
        let mut item = self.item_value.clone();
        item.init_from_slice(slice)?;
        Ok(item)
    }

    pub fn datum_copy_from_string(&self, string: &str, slice: &mut [u8]) -> result::Result<u32> {
        let mut item = self.item_value.clone();
        item.init_from_str(string)?;
        self.datum_copy_from_item(&item, slice)
    }

    pub fn datum_copy_from_item(
        &self,
        item: &dyn DatumItem,
        slice: &mut [u8],
    ) -> result::Result<u32> {
        item.to_slice(slice)
    }

    pub fn tuple_to_item(&self, tuple: &[u8]) -> result::Result<ItemValue> {
        let (offset, size) = self.tuple_get_offset_size(tuple);
        if offset + size > tuple.len() as u32 {
            panic!("error")
        }
        self.datum_to_item(&tuple[offset as usize..(offset + size) as usize])
    }

    pub fn tuple_set_datum(&self, tuple: &mut [u8], item: &dyn DatumItem) -> result::Result<u32> {
        let (offset, size) = self.tuple_get_offset_size(tuple);
        if offset + size > tuple.len() as u32 {
            panic!("error")
        }
        let new_size =
            self.datum_copy_from_item(item, &mut tuple[offset as usize..(offset + size) as usize])?;
        if !self.is_fixed_length {
            (self.fun_datum.fn_set_slot)(self.offset, offset, new_size, tuple)?;
        }
        Ok(size)
    }

    pub fn tuple_get_datum<'a>(&self, tuple: &'a [u8]) -> &'a [u8] {
        let (offset, size) = self.tuple_get_offset_size(tuple);
        (self.fun_datum.fn_get_datum)(offset, size, tuple)
    }

    pub fn tuple_get_offset_size(&self, tuple: &[u8]) -> (u32, u32) {
        if self.is_fixed_length {
            (self.offset, self.max_length)
        } else {
            let (offset, size) = (self.fun_datum.fn_get_slot)(self.offset, tuple);
            (offset, size)
        }
    }
}
