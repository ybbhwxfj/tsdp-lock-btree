use std::cmp::Ordering;

use std::str::FromStr;

use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::result::Result;

use crate::d_type::datum_item::DatumItem;

pub fn fn_cmp_f64(x: &[u8], y: &[u8]) -> Ordering {
    let fx = NetworkEndian::read_f64(x);
    let fy = NetworkEndian::read_f64(y);
    fx.partial_cmp(&fy).unwrap_or(Ordering::Equal)
}

pub fn fn_to_printable_f64(x: &[u8]) -> String {
    let x = NetworkEndian::read_f64(x);
    x.to_string()
}

pub fn fn_from_printable_f64(x: &mut [u8], s: &str) -> Result<u32> {
    let r = f64::from_str(s);
    if x.len() < std::mem::size_of::<f64>() {
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    let i = match r {
        Ok(i) => i,
        Err(e) => {
            return Err(ET::ParseError(e.to_string()));
        }
    };
    NetworkEndian::write_f64(x, i);
    Ok(std::mem::size_of::<f64>() as u32)
}

pub fn fn_to_item_f64(x: &[u8]) -> Box<dyn DatumItem> {
    let ix = NetworkEndian::read_f64(x);
    Box::new(DoubleItem::new(ix))
}

pub fn fn_from_item_f64(x: &mut [u8], item: &dyn DatumItem) -> Result<u32> {
    item.to_slice(x)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DoubleItem {
    double: f64,
}

impl DoubleItem {
    pub(crate) fn new(double: f64) -> Self {
        Self { double }
    }
}

impl DatumItem for DoubleItem {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        if slice.len() < std::mem::size_of::<f64>() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }
        let f = NetworkEndian::read_f64(slice);
        self.double = f;
        Ok(())
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        let r = f64::from_str(string);
        match r {
            Ok(i) => self.double = i,
            Err(e) => {
                return Err(ET::ParseError(e.to_string()));
            }
        };
        Ok(())
    }

    fn to_str(&self) -> Result<String> {
        Ok(self.double.to_string())
    }

    fn size(&self) -> u32 {
        std::mem::size_of::<f64>() as u32
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        if slice.len() < std::mem::size_of::<i64>() {
            assert!(false);
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        NetworkEndian::write_f64(slice, self.double);
        Ok(std::mem::size_of::<f64>() as u32)
    }

    fn to_vec(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.resize(self.size() as usize, 0);
        NetworkEndian::write_f64(vec.as_mut_slice(), self.double);
        return vec;
    }
}
