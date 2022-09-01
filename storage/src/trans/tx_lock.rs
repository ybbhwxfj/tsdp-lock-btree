use std::sync::atomic::Ordering;
use std::sync::Arc;

use atomic::Atomic;
use serde::Serialize;

use common::id::{OID, XID};

use crate::access::predicate::PredBase;
use crate::trans::lock_slot::LockType;
use crate::trans::tx_wait_notifier::TxWaitNotifier;

#[derive(Copy, Clone)]
pub struct EmptyFlag {
    pub is_read_row_empty: bool,
    pub is_write_row_empty: bool,
}

pub enum KeyOrPred {
    Key(Vec<u8>),
    Pred(Box<dyn PredBase>),
}

#[derive(Serialize)]
pub struct DebugTxLock {
    lock_id: OID,
    table_id: OID,
    lock_type: LockType,
}

pub struct TxLock {
    lock_id: OID,
    table_id: OID,
    lock_type: LockType,
    notifier: Arc<TxWaitNotifier>,
    opt_remove: Atomic<Option<EmptyFlag>>,
    payload: KeyOrPred, // a predicate or a key
}

impl TxLock {
    pub fn new(
        lock_id: OID,
        table_id: OID,
        lock_type: LockType,
        notifier: Arc<TxWaitNotifier>,
        payload: KeyOrPred,
    ) -> Self {
        Self {
            lock_id,
            table_id,
            lock_type,
            notifier,
            opt_remove: Atomic::new(None),
            payload,
        }
    }

    pub fn lock_id(&self) -> OID {
        self.lock_id
    }

    pub fn table_id(&self) -> OID {
        self.table_id
    }

    pub fn xid(&self) -> XID {
        self.notifier.xid()
    }

    pub fn notifier(&self) -> Arc<TxWaitNotifier> {
        self.notifier.clone()
    }

    pub fn lock_type(&self) -> LockType {
        self.lock_type
    }

    pub fn key(&self) -> Option<&Vec<u8>> {
        match &self.payload {
            KeyOrPred::Key(k) => Some(k),
            _ => None,
        }
    }

    pub fn set_deleted(&self) {
        match &self.payload {
            KeyOrPred::Pred(p) => p.set_deleted(),
            _ => {}
        }
    }

    pub fn set_opt_flag(&self, opt: Option<EmptyFlag>) {
        self.opt_remove.store(opt, Ordering::SeqCst);
    }

    pub fn get_opt_flag(&self) -> Option<EmptyFlag> {
        self.opt_remove.load(Ordering::SeqCst)
    }

    pub fn to_debug_serde(&self) -> DebugTxLock {
        DebugTxLock {
            lock_id: self.lock_id,
            table_id: self.table_id,
            lock_type: self.lock_type,
        }
    }
}

unsafe impl Sync for TxLock {}

unsafe impl Send for TxLock {}

impl EmptyFlag {
    pub fn is_empty(&self) -> bool {
        self.is_read_row_empty && self.is_write_row_empty
    }

    pub fn is_read_empty(&self) -> bool {
        self.is_read_row_empty
    }

    pub fn is_write_empty(&self) -> bool {
        self.is_write_row_empty
    }
}
