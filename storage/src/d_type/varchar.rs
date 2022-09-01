use std::cmp::Ordering;

use common::error_type::ET;

use crate::d_type::datum_item::DatumItem;
use crate::d_type::fixchar::CharItem;
use crate::d_type::write_string::write_string_to_buffer;

pub fn fn_cmp_string(x: &[u8], y: &[u8]) -> Ordering {
    let sx = std::str::from_utf8(x).unwrap();
    let sy = std::str::from_utf8(y).unwrap();
    sx.cmp(&sy)
}

pub fn fn_from_printable_string(x: &mut [u8], s: &str) -> Result<u32, ET> {
    write_string_to_buffer(x, s)?;
    Ok(s.len() as u32)
}

pub fn fn_to_printable_string(x: &[u8]) -> String {
    let sx = std::str::from_utf8(x).unwrap();
    let s = sx.trim_end_matches(char::from(0));
    s.to_string()
}

pub fn fn_to_item_string(x: &[u8]) -> Box<dyn DatumItem> {
    let sx = std::str::from_utf8(x).unwrap();
    let s = sx.trim_end_matches(char::from(0));

    Box::new(CharItem::new(s.to_string()))
}

pub fn fn_from_item_string(x: &mut [u8], item: &dyn DatumItem) -> Result<u32, ET> {
    item.to_slice(x)
}
