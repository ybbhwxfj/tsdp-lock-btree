use std::cmp::Ordering;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::result::Result;

use crate::d_type::datum_item::DatumItem;
use crate::d_type::write_string::write_string_to_buffer;

pub fn fn_cmp_char(x: &[u8], y: &[u8]) -> Ordering {
    let sx = std::str::from_utf8(x).unwrap();
    let sy = std::str::from_utf8(y).unwrap();
    sx.cmp(&sy)
}

pub fn fn_from_printable_char(x: &mut [u8], s: &str) -> Result<u32> {
    write_string_to_buffer(x, s)?;
    Ok(s.len() as u32)
}

pub fn fn_to_printable_char(x: &[u8]) -> String {
    let sx = std::str::from_utf8(x).unwrap();
    let s = sx.trim_end_matches(char::from(0));
    s.to_string()
}

pub fn fn_to_item_char(x: &[u8]) -> Box<dyn DatumItem> {
    let sx = std::str::from_utf8(x).unwrap();
    let s = sx.trim_end_matches(char::from(0));

    Box::new(CharItem::new(s.to_string()))
}

pub fn fn_from_item_char(x: &mut [u8], item: &dyn DatumItem) -> Result<u32> {
    item.to_slice(x)
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct CharItem {
    string: String,
}

impl CharItem {
    pub(crate) fn new(string: String) -> Self {
        Self { string }
    }
}

impl DatumItem for CharItem {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        let r = std::str::from_utf8(slice);
        let str = match r {
            Ok(str) => str,
            Err(_) => {
                return Err(ET::TODO);
            }
        };
        let s = str.trim_end_matches(char::from(0));
        self.string = String::from(s);
        Ok(())
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        self.string = String::from(string);
        Ok(())
    }

    fn to_str(&self) -> Result<String> {
        Ok(self.string.clone())
    }

    fn size(&self) -> u32 {
        self.string.len() as u32
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        write_string_to_buffer(slice, self.string.as_str())?;
        Ok(self.string.len() as u32)
    }

    fn to_vec(&self) -> Vec<u8> {
        let vec = Vec::from(self.string.as_str());
        return vec;
    }
}
