use std::any::Any;

use adt::slice::Slice;
use common::id::OID;
use common::result::Result;

pub trait PredKey = ?Sized + Send + Sync;

#[derive(Clone, Eq, PartialEq)]
pub enum PredType {
    PredEmpty,
    PredRange,
}

pub trait PredCtx: Clone + Send {}

pub trait PredTableAny: Any {}

pub trait PredTableBase: PredTableAny + Send {
    fn pred_type() -> PredType;
    fn default() -> Self;
}

pub trait PredBase: Any + Send {
    fn set_deleted(&self);
    fn is_deleted(&self) -> bool;
}

pub trait Pred<C: PredCtx>: PredBase + Clone {
    fn pred_type() -> PredType;
    fn contain<K: Slice>(&self, key: &K, ctx: &C) -> bool;
}

pub trait PredTable<C: PredCtx, P: Pred<C>>: PredTableBase {
    fn add(&mut self, oid: OID, pred: P);
    fn remove(&mut self, oid: &OID);
    fn merge(&mut self, other: &Self);

    /// find the predicate which contains the key
    /// the return value is optional a list of
    /// (a, b)
    ///     a. conflict predicate's oid
    ///     b. deleted predicate's oid for later garbage collection
    fn test_contain_and_deleted<K>(
        &self,
        key: &K,
        ctx: &C,
    ) -> Result<(Option<Vec<OID>>, Option<Vec<OID>>)>
    where
        K: Slice;
}

pub struct EmptyPredCtx {}

pub struct EmptyPred {}

pub struct EmptyPredTable {}

unsafe impl Send for EmptyPredTable {}

unsafe impl Sync for EmptyPredTable {}

impl Clone for EmptyPredTable {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl Clone for EmptyPredCtx {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl PredCtx for EmptyPredCtx {}

impl Clone for EmptyPred {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl PredBase for EmptyPred {
    fn set_deleted(&self) {}
    fn is_deleted(&self) -> bool {
        false
    }
}

impl<C: PredCtx> Pred<C> for EmptyPred {
    fn pred_type() -> PredType {
        PredType::PredEmpty
    }

    fn contain<K: Slice>(&self, _key: &K, _ctx: &C) -> bool {
        false
    }
}

impl PredTableAny for EmptyPredTable {}

impl PredTableBase for EmptyPredTable {
    fn pred_type() -> PredType {
        PredType::PredEmpty
    }

    fn default() -> Self {
        Self {}
    }
}

impl<C: PredCtx, P: Pred<C>> PredTable<C, P> for EmptyPredTable {
    fn add(&mut self, _oid: OID, _pred: P) {}

    fn remove(&mut self, _oid: &OID) {}

    fn merge(&mut self, _other: &Self) {}

    fn test_contain_and_deleted<K>(
        &self,
        _key: &K,
        _ctx: &C,
    ) -> Result<(Option<Vec<OID>>, Option<Vec<OID>>)>
    where
        K: Slice,
    {
        Ok((None, None))
    }
}
