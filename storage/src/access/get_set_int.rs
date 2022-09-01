use byteorder::{ByteOrder, NetworkEndian};

pub fn set_u32(ptr: *const u8, len: usize, offset: u32, value: u32) {
    if offset as usize + std::mem::size_of::<u32>() > len {
        panic!("set fail");
    }
    let buf = unsafe {
        let p = ptr as *mut u8;
        let s = p.add(offset as usize);
        std::slice::from_raw_parts_mut(s, std::mem::size_of::<u32>())
    };
    NetworkEndian::write_u32(buf, value)
}

pub fn get_u32(ptr: *const u8, len: usize, offset: u32) -> u32 {
    if offset as usize + std::mem::size_of::<u32>() > len {
        panic!("get fail");
    }
    let buf = unsafe {
        let s = ptr.add(offset as usize);
        std::slice::from_raw_parts(s, std::mem::size_of::<u32>())
    };
    NetworkEndian::read_u32(buf)
}

pub fn set_u128(ptr: *const u8, len: usize, offset: u32, value: u128) {
    if offset as usize + std::mem::size_of::<u32>() > len {
        panic!("set fail");
    }
    let buf = unsafe {
        let p = ptr as *mut u8;
        let s = p.add(offset as usize);
        std::slice::from_raw_parts_mut(s, std::mem::size_of::<u128>())
    };
    NetworkEndian::write_u128(buf, value)
}

pub fn get_u128(ptr: *const u8, len: usize, offset: u32) -> u128 {
    if offset as usize + std::mem::size_of::<u32>() > len {
        panic!("get fail");
    }
    let buf = unsafe {
        let s = ptr.add(offset as usize);
        std::slice::from_raw_parts(s, std::mem::size_of::<u128>())
    };
    NetworkEndian::read_u128(buf)
}
