use std::sync::Arc;

use async_std::sync::Mutex;
use scc::ebr::Barrier;
use scc::HashIndex;
use serde::Serialize;

use adt::compare::Compare;
use common::id::{OID, XID};
use common::result::Result;

use crate::access::to_json::ToJson;
use crate::storage_handler::StorageHandler;
use crate::trans::history::History;
use crate::trans::lock_slot::LockType;
use crate::trans::tx_lock::{DebugTxLock, TxLock};
use crate::trans::tx_state::TxState;
use crate::trans::tx_wait_notifier::TxWaitNotifier;
use crate::trans::write_operation::WriteOp;

#[derive(Serialize)]
pub struct DebugTxContext {
    xid: XID,
    locks: std::collections::HashMap<OID, DebugTxLock>,
    state: TxState,
}

pub struct TxContext {
    xid: XID,
    locks: HashIndex<OID, Arc<TxLock>>,
    notify: Arc<TxWaitNotifier>,
    write_set: Mutex<Vec<WriteOp>>,
}

#[derive(PartialEq, Eq)]
pub enum ReleaseType {
    ReleaseAll,
    ReleaseRead,
    ReleaseWrite,
}

impl TxContext {
    pub fn new(xid: XID) -> Self {
        Self {
            xid,
            locks: Default::default(),
            notify: Arc::new(TxWaitNotifier::new(xid)),
            write_set: Default::default(),
        }
    }
    pub fn xid(&self) -> XID {
        self.xid
    }

    pub fn xid_u32(&self) -> u32 {
        self.xid as u32
    }

    pub async fn release_lock<
        C: Compare<[u8]> + 'static,
        KT: ToJson + 'static,
        VT: ToJson + 'static,
        HI: History + 'static,
    >(
        &self,
        release_type: ReleaseType,
        handler: &StorageHandler<C, KT, VT, HI>,
    ) -> Result<()> {
        let mut locks = Vec::new();
        {
            let barrier = Barrier::new();

            for (id, l) in self.locks.iter(&barrier) {
                match release_type {
                    ReleaseType::ReleaseAll => {
                        locks.push((*id, l.clone()));
                    }
                    ReleaseType::ReleaseRead => match l.lock_type() {
                        LockType::LockKeyRead | LockType::LockPredicate => {
                            locks.push((*id, l.clone()));
                        }
                        _ => {}
                    },
                    ReleaseType::ReleaseWrite => match l.lock_type() {
                        LockType::LockKeyWrite => {
                            locks.push((*id, l.clone()));
                        }
                        _ => {}
                    },
                }
            }
        }

        // call release
        for (id, l) in locks {
            handler.release_lock(l).await?;
            let _ = self.locks.remove_async(&id).await;
        }
        Ok(())
    }

    pub fn notifier(&self) -> Arc<TxWaitNotifier> {
        self.notify.clone()
    }

    pub async fn add_lock(&self, tx_lock: Arc<TxLock>) {
        let opt = self.locks.insert_async(tx_lock.lock_id(), tx_lock).await;
        assert!(opt.is_ok());
    }

    pub async fn add_write_op(&self, write_op: WriteOp) {
        let mut guard = self.write_set.lock().await;
        guard.push(write_op);
    }

    pub async fn get_write_op(&self) -> Vec<WriteOp> {
        let mut write_ops = self.write_set.lock().await;
        let mut ret = Vec::new();
        std::mem::swap(&mut (*write_ops), &mut ret);
        return ret;
    }

    pub async fn get_state(&self) -> TxState {
        self.notify.get_state().await
    }

    pub async fn notify_abort(&self, r: Result<()>) {
        self.notify.notify_abort(r).await;
    }

    pub async fn to_debug_serde(&self) -> DebugTxContext {
        let locks = {
            let barrier = Barrier::new();
            let mut locks = std::collections::HashMap::default();
            for (id, l) in self.locks.iter(&barrier) {
                let d = l.to_debug_serde();
                let _ = locks.insert(id.clone(), d);
            }
            locks
        };
        DebugTxContext {
            xid: self.xid,
            locks,
            state: self.notify.get_state().await,
        }
    }
}
