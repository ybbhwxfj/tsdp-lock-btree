use std::cmp::Ordering;
use std::str::FromStr;

use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::result::Result;

use crate::d_type::datum_item::DatumItem;

pub fn fn_cmp_f32(x: &[u8], y: &[u8]) -> Ordering {
    let fx = NetworkEndian::read_f32(x);
    let fy = NetworkEndian::read_f32(y);
    fx.partial_cmp(&fy).unwrap_or(Ordering::Equal)
}

pub fn fn_to_printable_f32(x: &[u8]) -> String {
    let x = NetworkEndian::read_f32(x);
    x.to_string()
}

pub fn fn_from_printable_f32(x: &mut [u8], s: &str) -> Result<u32> {
    if x.len() < std::mem::size_of::<f32>() {
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    let r = f32::from_str(s);
    let i = match r {
        Ok(i) => i,
        Err(e) => {
            return Err(ET::ParseError(e.to_string()));
        }
    };
    NetworkEndian::write_f32(x, i);
    Ok(std::mem::size_of::<f32>() as u32)
}

pub fn fn_to_item_f32(x: &[u8]) -> Box<dyn DatumItem> {
    let f = NetworkEndian::read_f32(x);
    Box::new(FloatItem::new(f))
}

pub fn fn_from_item_f32(x: &mut [u8], item: &dyn DatumItem) -> Result<u32> {
    item.to_slice(x)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FloatItem {
    float: f32,
}

impl FloatItem {
    pub fn new(float: f32) -> Self {
        Self { float }
    }
}

impl DatumItem for FloatItem {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        if slice.len() < std::mem::size_of::<f32>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }
        let f = NetworkEndian::read_f32(slice);
        self.float = f;
        Ok(())
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        let r = f32::from_str(string);
        match r {
            Ok(i) => self.float = i,
            Err(e) => {
                return Err(ET::ParseError(e.to_string()));
            }
        };
        Ok(())
    }

    fn to_str(&self) -> Result<String> {
        Ok(self.float.to_string())
    }

    fn size(&self) -> u32 {
        std::mem::size_of::<f32>() as u32
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        if slice.len() < std::mem::size_of::<f32>() {
            assert!(false);
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        NetworkEndian::write_f32(slice, self.float);
        Ok(std::mem::size_of::<f32>() as u32)
    }

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.resize(self.size() as usize, 0);
        NetworkEndian::write_f32(vec.as_mut_slice(), self.float);
        return vec;
    }
}
