use std::cell::RefCell;
use std::cmp::Ordering;

use common::result::Result;

use crate::d_type::datum_item::DatumItem;

pub type FnDefaultItem = fn() -> Box<RefCell<dyn DatumItem>>;
pub type FnCompare = fn(datum1: &[u8], datum2: &[u8]) -> Ordering;
pub type FnGetDatum = for<'x> fn(offset: u32, length: u32, tuple: &'x [u8]) -> &'x [u8];
pub type FnSetDatum = fn(offset: u32, src: &[u8], tuple: &mut [u8]) -> Result<()>;
pub type FnFromString = fn(datum: &mut [u8], string: &str) -> Result<u32>;
pub type FnToString = fn(datum: &[u8]) -> String;
pub type FnToItem = fn(datum: &[u8]) -> Box<dyn DatumItem>;
pub type FnFromItem = fn(datum: &mut [u8], item: &dyn DatumItem) -> Result<u32>;
pub type FnGetSlot = fn(slot_offset: u32, tuple: &[u8]) -> (u32, u32);
pub type FnSetSlot = fn(slot_offset: u32, offset: u32, size: u32, tuple: &mut [u8]) -> Result<()>;
