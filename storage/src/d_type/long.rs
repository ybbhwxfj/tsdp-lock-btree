use std::cmp::Ordering;
use std::str::FromStr;

use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::result::Result;

use crate::d_type::datum_item::DatumItem;

pub fn fn_cmp_i64(x: &[u8], y: &[u8]) -> Ordering {
    let ix = NetworkEndian::read_i64(x);
    let iy = NetworkEndian::read_i64(y);
    ix.cmp(&iy)
}

pub fn fn_from_printable_i64(x: &mut [u8], s: &str) -> Result<u32> {
    if x.len() < std::mem::size_of::<i64>() {
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    let r = i64::from_str(s);
    let i = match r {
        Ok(i) => i,
        Err(e) => {
            return Err(ET::ParseError(e.to_string()));
        }
    };
    NetworkEndian::write_i64(x, i);
    Ok(std::mem::size_of::<i64>() as u32)
}

pub fn fn_to_printable_i64(x: &[u8]) -> String {
    let x = NetworkEndian::read_i64(x);
    x.to_string()
}

pub fn fn_to_item_i64(x: &[u8]) -> Box<dyn DatumItem> {
    let ix = NetworkEndian::read_i64(x);
    Box::new(LongItem::new(ix))
}

pub fn fn_from_item_i64(x: &mut [u8], item: &dyn DatumItem) -> Result<u32> {
    item.to_slice(x)
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct LongItem {
    long: i64,
}

impl LongItem {
    pub fn new(long: i64) -> Self {
        Self { long }
    }
}

impl DatumItem for LongItem {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        if slice.len() < std::mem::size_of::<i64>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }
        let i = NetworkEndian::read_i64(slice);
        self.long = i;
        Ok(())
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        let r = i64::from_str(string);
        match r {
            Ok(i) => self.long = i,
            Err(e) => {
                return Err(ET::ParseError(e.to_string()));
            }
        };
        Ok(())
    }

    fn to_str(&self) -> Result<String> {
        Ok(self.long.to_string())
    }

    fn size(&self) -> u32 {
        std::mem::size_of::<i64>() as u32
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        if slice.len() < std::mem::size_of::<i64>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        NetworkEndian::write_i64(slice, self.long);
        Ok(std::mem::size_of::<i64>() as u32)
    }

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.resize(self.size() as usize, 0);
        NetworkEndian::write_i64(vec.as_mut_slice(), self.long);
        return vec;
    }
}
