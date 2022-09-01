pub trait Slice {
    fn as_slice(&self) -> &[u8];
}

pub trait FromSlice {
    fn from_slice(_: &[u8]) -> Self;
}

pub trait Equal<S: ?Sized>: Clone {
    fn equal(&self, k1: &S, k2: &S) -> bool;
}

pub trait Hash<S: ?Sized>: Clone {
    fn hash(&self, k: &S) -> u64;
}

pub struct SliceRef<'a> {
    reference: &'a [u8],
}

impl<'a> SliceRef<'a> {
    pub fn new(reference: &'a [u8]) -> Self {
        Self { reference }
    }
}

impl<'a> Clone for SliceRef<'a> {
    fn clone(&self) -> Self {
        Self {
            reference: self.reference,
        }
    }
}

impl<'a> Slice for SliceRef<'a> {
    fn as_slice(&self) -> &[u8] {
        self.reference
    }
}

impl Slice for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self
    }
}

impl Slice for &[u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }
}

impl Slice for [u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }
}

pub struct EmptySlice {
    slice: [u8; 0],
}

impl Slice for EmptySlice {
    fn as_slice(&self) -> &[u8] {
        &self.slice
    }
}
