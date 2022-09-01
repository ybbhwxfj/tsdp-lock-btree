use crate::access::page_id::PageId;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Upsert {
    Update,
    Insert,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ResultUpsert {
    // could not find a position, search the right link page
    SearchRight(PageId),
    // this page is full, must split, optional insert position
    NeedSplit(u32),
    // Update Or Insert Ok, if the old high key has
    UpsertOk,

    NonExistingKey,

    ExistingKey,

    // restart this search from start of the page list
    SearchFromStart,
}
