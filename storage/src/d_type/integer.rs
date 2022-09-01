use std::cmp::Ordering;
use std::str::FromStr;

use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::result::Result;

use crate::d_type::datum_item::DatumItem;

pub fn fn_cmp_i32(x: &[u8], y: &[u8]) -> Ordering {
    let ix = NetworkEndian::read_i32(x);
    let iy = NetworkEndian::read_i32(y);
    ix.cmp(&iy)
}

pub fn fn_from_printable_i32(x: &mut [u8], s: &str) -> Result<u32> {
    if x.len() < std::mem::size_of::<i32>() {
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    let r = i32::from_str(s);
    let i = match r {
        Ok(i) => i,
        Err(e) => {
            return Err(ET::ParseError(e.to_string()));
        }
    };

    NetworkEndian::write_i32(x, i);
    Ok(std::mem::size_of::<i32>() as u32)
}

pub fn fn_to_printable_i32(x: &[u8]) -> String {
    let x = NetworkEndian::read_i32(x);
    x.to_string()
}

pub fn fn_to_item_i32(x: &[u8]) -> Box<dyn DatumItem> {
    let ix = NetworkEndian::read_i32(x);
    Box::new(IntegerItem::new(ix))
}

pub fn fn_from_item_i32(x: &mut [u8], item: &dyn DatumItem) -> Result<u32> {
    item.to_slice(x)
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct IntegerItem {
    integer: i32,
}

impl IntegerItem {
    pub fn new(integer: i32) -> Self {
        Self { integer }
    }
}

impl DatumItem for IntegerItem {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        if slice.len() < std::mem::size_of::<i32>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }
        let i = NetworkEndian::read_i32(slice);
        self.integer = i;
        Ok(())
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        let r = i32::from_str(string);
        let i = match r {
            Ok(i) => i,
            Err(e) => {
                return Err(ET::ParseError(e.to_string()));
            }
        };
        self.integer = i;
        Ok(())
    }

    fn to_str(&self) -> Result<String> {
        Ok(self.integer.to_string())
    }

    fn size(&self) -> u32 {
        std::mem::size_of::<i32>() as u32
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        if slice.len() < std::mem::size_of::<i32>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        NetworkEndian::write_i32(slice, self.integer);
        Ok(std::mem::size_of::<i32>() as u32)
    }

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.resize(self.size() as usize, 0);
        NetworkEndian::write_i32(vec.as_mut_slice(), self.integer);
        return vec;
    }
}
