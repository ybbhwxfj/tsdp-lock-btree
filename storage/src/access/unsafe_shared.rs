#[derive(Clone)]
pub struct UnsafeShared<T: ?Sized> {
    ptr: *mut T,
}

impl<T: ?Sized> UnsafeShared<T> {
    pub fn new(ptr: *mut T) -> Self {
        UnsafeShared { ptr }
    }

    pub fn const_ptr(&self) -> *const T {
        self.ptr
    }

    pub fn mut_ptr(&mut self) -> *mut T {
        self.ptr
    }
}

unsafe impl<T: ?Sized + Sync + Send> Send for UnsafeShared<T> {}

unsafe impl<T: ?Sized + Sync + Send> Sync for UnsafeShared<T> {}
