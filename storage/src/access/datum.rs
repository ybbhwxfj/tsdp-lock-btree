use std::cmp::Ordering;

use common::result::Result;

use crate::access::datum_oper::DatumOper;
use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;

pub struct Datum<'p> {
    slice: &'p [u8],
}

pub struct DatumMut<'p> {
    slice: &'p mut [u8],
}

impl<'p> Datum<'p> {
    pub fn new(slice: &'p [u8]) -> Datum {
        Datum { slice }
    }

    pub fn slice(&'p self) -> &'p [u8] {
        self.slice
    }

    pub fn cmp(&self, other: &Self, desc: &DatumOper) -> Ordering {
        desc.datum_compare(self.slice, other.slice)
    }

    pub fn to_string(&self, desc: &DatumOper) -> Result<String> {
        desc.datum_to_string(self.slice)
    }

    pub fn to_item(&self, desc: &DatumOper) -> Result<ItemValue> {
        desc.datum_to_item(self.slice)
    }
}

impl<'p> DatumMut<'p> {
    pub fn new(slice: &'p mut [u8]) -> DatumMut<'p> {
        DatumMut { slice }
    }

    pub fn slice(&'p mut self) -> &'p [u8] {
        self.slice
    }

    pub fn mut_slice(&'p mut self) -> &'p mut [u8] {
        self.slice
    }

    pub fn cmp(&self, other: &Self, desc: &DatumOper) -> Ordering {
        desc.datum_compare(self.slice, other.slice)
    }

    pub fn to_string(&self, desc: &DatumOper) -> Result<String> {
        desc.datum_to_string(self.slice)
    }

    pub fn copy_from_item(&mut self, item: &dyn DatumItem) -> Result<u32> {
        item.to_slice(self.slice)
    }

    pub fn copy_from_string(&mut self, s: &String, op: &DatumOper) -> Result<u32> {
        op.datum_copy_from_string(s.as_str(), self.slice)
    }
}
