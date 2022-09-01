use common::error_type::ET;
use common::result::Result;

use crate::access::const_val;
use crate::access::datum::DatumMut;

use crate::access::datum_oper::DatumOper;
use crate::access::datum_slot::DatumSlotMut;
use crate::access::get_set_int::{get_u32, set_u32};

use crate::access::tuple_oper::TupleOper;
use crate::access::tuple_raw::TUPLE_SLOT_SIZE;
use crate::access::update_tuple::UpdateTuple;
use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;

pub struct TupleRawMut<'p> {
    slice: &'p mut [u8],
}

impl<'p> TupleRawMut<'p> {
    pub fn from_slice(s: &'p mut [u8]) -> TupleRawMut {
        TupleRawMut { slice: s }
    }

    pub fn from_typed_items(
        vec: &'p mut [u8],
        desc: &TupleOper,
        item: &Vec<ItemValue>,
    ) -> Result<TupleRawMut<'p>> {
        Self::from_items_gut(vec, desc, item)
    }

    fn from_items_gut<T: DatumItem>(
        vec: &'p mut [u8],
        desc: &TupleOper,
        item: &Vec<T>,
    ) -> Result<TupleRawMut<'p>>
    where
        T: DatumItem,
    {
        let mut tuple = TupleRawMut::from_slice(vec);
        // item and datum desc have the same size
        if item.len() != desc.datum_oper().len() {
            return Err(ET::ErrorLength);
        }
        let mut var_len_size = 0;
        for (i, d) in desc.datum_oper().iter().enumerate() {
            let item = match item.get(i) {
                Some(item) => item,
                None => {
                    panic!("not possible")
                }
            };

            if d.is_fixed_length() {
                let mut datum = tuple.get_datum_by_offset_mut(d.offset());
                let _ = datum.copy_from_item(item)?;
            } else {
                let offset = var_len_size + desc.slot_offset_end();
                let mut slot = tuple.get_slot_mut(d.offset());
                let mut datum = tuple.get_datum_by_offset_mut(offset);

                let len = datum.copy_from_item(item)?;
                var_len_size += len;
                slot.set_offset(offset);
                slot.set_size(len);
            }
        }
        let size = var_len_size + desc.slot_offset_end();
        let mut tuple = TupleRawMut::from_slice(&mut vec[0..size as usize]);
        tuple.set_size(size);
        tuple.set_size(size); // TODO
        Ok(tuple)
    }

    pub fn len(&self) -> u32 {
        self.slice.len() as u32
    }

    pub fn as_slice(&self) -> &[u8] {
        self.slice
    }

    pub fn set_size(&mut self, size: u32) {
        set_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            const_val::TUPLE_SIZE_OFFSET,
            size,
        );
    }

    pub fn get_size(&mut self) -> u32 {
        get_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            const_val::TUPLE_SIZE_OFFSET,
        )
    }

    fn get_slot_mut(&mut self, offset: u32) -> DatumSlotMut<'p> {
        let slice = unsafe {
            let ptr = self.slice.as_ptr().add(offset as usize) as *mut u8;
            std::slice::from_raw_parts_mut(ptr, TUPLE_SLOT_SIZE as usize)
        };
        DatumSlotMut::new(slice)
    }

    pub fn get_datum_by_offset_mut(&mut self, offset: u32) -> DatumMut<'p> {
        let slice = unsafe {
            let ptr = self.slice.as_ptr().add(offset as usize) as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.slice.len() - offset as usize)
        };
        DatumMut::new(slice)
    }

    pub fn from_string(&'p mut self, s: &String, desc: &DatumOper, offset: u32) -> Result<u32> {
        if desc.is_fixed_length() {
            self.from_string_fixed_length(s, desc)
        } else {
            self.from_string_var_length(s, desc, offset)
        }
    }

    pub fn from_string_fixed_length(&mut self, s: &String, desc: &DatumOper) -> Result<u32> {
        let mut kd = self.get_datum_by_offset_size_mut(desc.offset(), desc.max_length());
        kd.copy_from_string(s, desc)
    }

    pub fn from_string_var_length(
        &mut self,
        s: &String,
        desc: &DatumOper,
        var_offset: u32,
    ) -> Result<u32> {
        let size = {
            let mut kd = self.get_datum_by_offset_mut(var_offset);
            let result = kd.copy_from_string(s, desc);
            match result {
                Ok(len) => len,
                Err(_e) => 0, // *TODO
            }
        };

        let size = {
            let mut s: DatumSlotMut<'p> = self.get_slot_mut(desc.offset());
            s.set_offset(var_offset);
            s.set_size(size);
            size
        };
        Ok(size)
    }

    pub fn get_datum_by_offset_size_mut(&'p self, offset: u32, size: u32) -> DatumMut<'p> {
        let slice = unsafe {
            let ptr = self.slice.as_ptr().add(offset as usize) as *mut u8;
            std::slice::from_raw_parts_mut(ptr, (offset + size) as usize)
        };
        DatumMut::new(slice)
    }

    pub fn tuple_update(
        &mut self,
        tuple_oper: &TupleOper,
        update: &UpdateTuple,
    ) -> Result<Option<Vec<u8>>> {
        update.update(self.slice, tuple_oper)
    }
}
