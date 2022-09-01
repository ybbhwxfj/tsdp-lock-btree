use std::mem::size_of;

use byteorder::{ByteOrder, NetworkEndian};

pub struct DatumSlot<'p> {
    slice: &'p [u8],
}

pub struct DatumSlotMut<'p> {
    slice: &'p mut [u8],
}

impl<'p> DatumSlot<'p> {
    pub fn new(bin: &'p [u8]) -> DatumSlot<'p> {
        DatumSlot { slice: bin }
    }
    pub fn get_offset(&self) -> u32 {
        NetworkEndian::read_u32(&self.slice[0..size_of::<u32>()])
    }

    pub fn get_size(&self) -> u32 {
        NetworkEndian::read_u32(&self.slice[size_of::<u32>()..size_of::<u32>() * 2])
    }
}

impl<'p> DatumSlotMut<'p> {
    pub fn new(bin: &'p mut [u8]) -> DatumSlotMut<'p> {
        DatumSlotMut { slice: bin }
    }
    pub fn set_offset(&mut self, offset: u32) {
        NetworkEndian::write_u32(&mut self.slice[0..std::mem::size_of::<u32>()], offset)
    }

    pub fn set_size(&mut self, size: u32) {
        NetworkEndian::write_u32(
            &mut self.slice[std::mem::size_of::<u32>()..std::mem::size_of::<u32>() * 2],
            size,
        )
    }
}
