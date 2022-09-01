use json::{array, object, JsonValue};

use common::error_type::ET;

use crate::access::const_val;
use crate::access::datum::Datum;

use crate::access::datum_oper::DatumOper;
use crate::access::get_set_int::get_u32;

use crate::access::tuple_oper::TupleOper;
use crate::d_type::datum_item::DatumItem;

pub const TUPLE_SLOT_SIZE: u32 = 8;

pub struct TupleRaw<'p> {
    slice: &'p [u8],
}

impl<'p> Clone for TupleRaw<'p> {
    fn clone(&self) -> Self {
        TupleRaw::from_slice(self.slice)
    }
}

impl<'p> TupleRaw<'p> {
    pub fn from_slice(bin: &'p [u8]) -> TupleRaw<'p> {
        TupleRaw { slice: bin }
    }

    pub fn get_size(&mut self) -> u32 {
        let n = get_u32(
            self.slice.as_ptr(),
            self.slice.len(),
            const_val::TUPLE_HEADER_SIZE,
        );
        assert!(n as usize <= self.slice.len());
        n
    }

    pub fn get_datum(&'p self, desc: &DatumOper) -> Datum<'p> {
        Datum::new(desc.tuple_get_datum(self.slice))
    }

    pub fn len(&self) -> u32 {
        self.slice.len() as u32
    }

    pub fn slice(&'p self) -> &'p [u8] {
        self.slice
    }

    pub fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    pub fn copy_from<'b>(&mut self, tuple: &TupleRaw<'b>) -> Result<(), ET> {
        if tuple.slice.len() > self.slice.len() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        } else {
            unsafe {
                std::ptr::copy(
                    tuple.slice.as_ptr(),
                    self.slice.as_ptr() as *mut u8,
                    tuple.slice.len(),
                )
            }
        }
        Ok(())
    }

    pub fn to_json(&self, desc: &TupleOper) -> Result<JsonValue, ET> {
        let mut array = array![];
        if self.is_empty() {
            return Ok(object! {
                "tuple": 0,
            });
        }
        for d in desc.datum_oper().iter() {
            let datum = self.get_datum(d);
            let s = datum.to_item(d)?;
            let v = s.to_str()?;
            match array.push(v) {
                Ok(()) => {}
                Err(e) => {
                    return Err(ET::JSONError(e.to_string()));
                }
            }
        }
        Ok(array)
    }
}
