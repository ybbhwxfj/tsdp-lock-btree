use common::error_type::ET;

pub fn write_string_to_buffer(x: &mut [u8], s: &str) -> Result<(), ET> {
    if x.len() < s.len() {
        assert!(false);
        assert!(false);
        return Err(ET::ExceedCapacity);
    }
    unsafe {
        std::ptr::copy(s.as_ptr(), x.as_mut_ptr(), s.len());
        std::ptr::write_bytes(x.as_mut_ptr().add(s.len()), 0, x.len() - s.len())
    }
    Ok(())
}
