use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use adt::compare::Compare;
use adt::range::{RangeBounds, RangeContain};
use adt::slice::Slice;
use common::id::OID;
use common::result::Result;

use crate::access::predicate::{
    Pred, PredBase, PredCtx, PredTable, PredTableAny, PredTableBase, PredType,
};

pub trait KeyCmp = Compare<[u8]> + 'static;

pub struct BtPred {
    range: RangeBounds<Vec<u8>>,
    deleted: Arc<AtomicBool>,
}

pub struct BtPredTable {
    table: HashMap<OID, BtPred>,
}

pub struct BtPredCtx<C: KeyCmp> {
    cmp: C,
}

impl<C: KeyCmp> Clone for BtPredCtx<C> {
    fn clone(&self) -> Self {
        Self {
            cmp: self.cmp.clone(),
        }
    }
}

impl<C: KeyCmp> PredCtx for BtPredCtx<C> {}

impl Clone for BtPred {
    fn clone(&self) -> Self {
        let fn_clone = |x: &Vec<u8>| -> Vec<u8> { x.clone() };
        Self {
            range: self.range.map(&fn_clone),
            deleted: self.deleted.clone(),
        }
    }
}

impl PredBase for BtPred {
    fn set_deleted(&self) {
        self.deleted.store(true, Ordering::Relaxed);
    }

    fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Relaxed)
    }
}

impl<C: KeyCmp> Pred<BtPredCtx<C>> for BtPred {
    fn pred_type() -> PredType {
        PredType::PredRange
    }

    fn contain<K: Slice>(&self, key: &K, ctx: &BtPredCtx<C>) -> bool {
        self.range.contain(key.as_slice(), &ctx.cmp) == RangeContain::Contain
    }
}

impl PredTableAny for BtPredTable {}

impl PredTableBase for BtPredTable {
    fn pred_type() -> PredType {
        PredType::PredRange
    }

    fn default() -> Self {
        Self {
            table: HashMap::new(),
        }
    }
}

impl<C: KeyCmp> PredTable<BtPredCtx<C>, BtPred> for BtPredTable {
    fn add(&mut self, oid: OID, pred: BtPred) {
        self.table.insert(oid, pred);
    }

    fn remove(&mut self, oid: &OID) {
        self.table.remove(oid);
    }

    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.table {
            if !v.is_deleted() {
                self.table.insert(k.clone(), v.clone());
            }
        }
    }

    fn test_contain_and_deleted<K>(
        &self,
        key: &K,
        ctx: &BtPredCtx<C>,
    ) -> Result<(Option<Vec<OID>>, Option<Vec<OID>>)>
    where
        K: Slice,
    {
        let mut deleted = Vec::new();
        let mut conflict = Vec::new();
        for (k, v) in self.table.iter() {
            if v.is_deleted() {
                deleted.push(*k);
                continue;
            } else {
                let is_contain = v.contain(key, ctx);
                if is_contain {
                    conflict.push(*k);
                }
            }
        }
        let r1 = if conflict.is_empty() {
            None
        } else {
            Some(conflict)
        };
        let r2 = if deleted.is_empty() {
            None
        } else {
            Some(deleted)
        };
        return Ok((r1, r2));
    }
}

impl<C: KeyCmp> BtPredCtx<C> {
    pub fn new(cmp: C) -> Self {
        Self { cmp }
    }
}

impl BtPred {
    pub fn from<K: Slice>(range: &RangeBounds<K>) -> Self {
        let f = |key: &K| -> Vec<u8> { Vec::from(key.as_slice()) };
        let search_range = range.map(&f);
        Self::new(search_range)
    }

    pub fn new(range: RangeBounds<Vec<u8>>) -> Self {
        Self {
            range,
            deleted: Arc::new(Default::default()),
        }
    }

    pub fn range(&self) -> &RangeBounds<Vec<u8>> {
        &self.range
    }
}
