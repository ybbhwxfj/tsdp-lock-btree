use std::hash::Hash;

use common::result::Result;

/// dynamic datum
pub trait DatumItem: Send + Sync {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()>;
    fn init_from_str(&mut self, string: &str) -> Result<()>;
    fn to_str(&self) -> Result<String>;
    fn size(&self) -> u32;
    fn to_slice(&self, _: &mut [u8]) -> Result<u32>;
    fn to_vec(&self) -> Vec<u8>;
}

pub trait ComparableItem: DatumItem + Eq + Hash + Ord {}
