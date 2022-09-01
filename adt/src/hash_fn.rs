pub trait HashFn<S: ?Sized>: Clone {
    fn hash(&self, k: &S) -> u64;
}
