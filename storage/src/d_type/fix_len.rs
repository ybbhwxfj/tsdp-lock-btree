use common::error_type::ET;
use common::result::Result;

pub fn fn_get_datum(offset: u32, length: u32, tuple: &[u8]) -> &[u8] {
    let off = offset as usize;
    let len = length as usize;
    &tuple[off..off + len]
}

pub fn fn_set_datum(offset: u32, src: &[u8], tuple: &mut [u8]) -> Result<()> {
    let offset = offset as usize;
    if offset + src.len() > tuple.len() {
        assert!(false);
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    tuple[offset..offset + src.len()].copy_from_slice(src);
    Ok(())
}
