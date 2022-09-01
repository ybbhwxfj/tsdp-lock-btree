use std::ops::Bound;

use crate::compare::{Compare, IsEqual};
use crate::slice::Slice;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct RangeBounds<T> {
    start: Bound<T>,
    end: Bound<T>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum RangeContain {
    NotContainLeft,
    NotContainRight,
    Contain,
}

/// `RangeBounds` is
impl<T> RangeBounds<T> {
    pub fn new(start: Bound<T>, end: Bound<T>) -> Self {
        Self { start, end }
    }
    /// Start index bound.
    ///
    /// Returns the start value as a `Bound`.
    ///
    pub fn low(&self) -> &Bound<T> {
        &self.start
    }

    /// End index bound.
    ///
    /// Returns the end value as a `Bound`.
    ///
    pub fn up(&self) -> &Bound<T> {
        &self.end
    }

    /// Returns `true` if `item` is contained in the range.
    ///
    pub fn contain<C>(&self, item: &[u8], cmp: &C) -> RangeContain
    where
        T: Slice,
        C: Compare<[u8]>,
    {
        let ok1 = Self::contains_bound::<true, C>(self.low(), item, cmp);
        let ok2 = Self::contains_bound::<false, C>(self.up(), item, cmp);
        if ok1 && ok2 {
            RangeContain::Contain
        } else if ok2 {
            RangeContain::NotContainLeft
        } else {
            RangeContain::NotContainRight
        }
    }

    pub fn map<E, M>(&self, fn_map: &M) -> RangeBounds<E>
    where
        M: Fn(&T) -> E,
    {
        let low = Self::map_bound(self.low(), fn_map);
        let up = Self::map_bound(self.up(), fn_map);
        RangeBounds::new(low, up)
    }

    fn map_bound<E, M>(bound: &Bound<T>, f: &M) -> Bound<E>
    where
        M: Fn(&T) -> E,
    {
        match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(x) => Bound::Included(f(x)),
            Bound::Excluded(x) => Bound::Excluded(f(x)),
        }
    }
    fn contains_bound<const START: bool, C>(bound: &Bound<T>, item: &[u8], cmp: &C) -> bool
    where
        T: Slice,
        C: Compare<[u8]>,
    {
        match bound {
            Bound::Included(start_or_end) => {
                let ord = if START {
                    cmp.compare(start_or_end.as_slice(), item)
                } else {
                    cmp.compare(item, start_or_end.as_slice())
                };
                ord.is_le()
            }
            Bound::Excluded(start_or_end) => {
                let ord = if START {
                    cmp.compare(start_or_end.as_slice(), item)
                } else {
                    cmp.compare(item, start_or_end.as_slice())
                };
                ord.is_lt()
            }
            Bound::Unbounded => true,
        }
    }
}

pub fn bound_eq<T: Slice, C: IsEqual<T>>(b1: &Bound<T>, b2: &Bound<T>, cmp: &C) -> bool {
    match (b1, b2) {
        (Bound::Included(v1), Bound::Included(v2)) => cmp.equal(v1, v2),
        (Bound::Excluded(v1), Bound::Excluded(v2)) => cmp.equal(v1, v2),
        (Bound::Unbounded, Bound::Unbounded) => true,
        _ => false,
    }
}

pub fn range_bounds_eq<T: Slice, C: IsEqual<T>>(
    r1: &RangeBounds<T>,
    r2: &RangeBounds<T>,
    equal: &C,
) -> bool {
    bound_eq(r1.low(), r2.low(), equal) && bound_eq(r1.up(), r2.up(), equal)
}
