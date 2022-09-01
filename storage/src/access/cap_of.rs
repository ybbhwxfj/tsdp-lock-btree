/// calculate the capacity of a size
pub fn capacity_of(size: u32) -> u32 {
    if size <= 1 {
        size
    } else if size <= (1 << 8) {
        (u32::MAX >> (size - 1).leading_zeros()) + 1
    } else {
        let n = size % 1024;
        if n == 0 {
            size
        } else {
            (n + 1) * 1024
        }
    }
}
