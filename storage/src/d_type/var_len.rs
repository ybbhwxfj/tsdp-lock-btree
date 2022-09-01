use common::result::Result;

use crate::access::datum_slot::{DatumSlot, DatumSlotMut};
use crate::access::tuple_raw::TUPLE_SLOT_SIZE;

pub fn fn_get_slot(slot_offset: u32, tuple: &[u8]) -> (u32, u32) {
    let off = slot_offset as usize;
    let slot = DatumSlot::new(&tuple[off..off + TUPLE_SLOT_SIZE as usize]);
    (slot.get_offset(), slot.get_size())
}

pub fn fn_set_slot(slot_offset: u32, offset: u32, size: u32, tuple: &mut [u8]) -> Result<()> {
    let p = slot_offset as usize;
    let mut slot = DatumSlotMut::new(&mut tuple[p..p + TUPLE_SLOT_SIZE as usize]);
    slot.set_size(size);
    slot.set_offset(offset);
    Ok(())
}
