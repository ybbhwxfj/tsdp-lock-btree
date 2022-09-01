use std::cmp::Ordering;

pub trait IsEqual<S: ?Sized>: Clone + Send + Sync {
    fn equal(&self, k1: &S, k2: &S) -> bool;
}

pub trait Compare<S: ?Sized>: Clone + Send + Sync {
    fn compare(&self, k1: &S, k2: &S) -> Ordering;
}
