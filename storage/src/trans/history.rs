use async_trait::async_trait;

use adt::range::RangeBounds;
use common::id::{OID, XID};

pub enum TxHistOp {
    // (table id, operation id, write key)
    THWrite(OID, OID, Vec<u8>),
    // (table id, operation id, read key)
    THRead(OID, OID, Vec<u8>),
    // (table id, operation id, search range)
    THRange(OID, OID, Option<RangeBounds<Vec<u8>>>),
    THCommit,
    THAbort,
}

#[async_trait]
pub trait History: Send + Sync + Clone {
    async fn append(&self, xid: XID, op: TxHistOp);
}

pub struct HistoryEmpty {}

impl Clone for HistoryEmpty {
    fn clone(&self) -> Self {
        Self {}
    }
}

#[async_trait]
impl History for HistoryEmpty {
    async fn append(&self, _xid: XID, _op: TxHistOp) {}
}

impl HistoryEmpty {
    pub fn new() -> Self {
        Self {}
    }
}
