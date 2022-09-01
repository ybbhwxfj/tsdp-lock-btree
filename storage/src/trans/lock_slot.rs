use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use tracing::trace;

use common::id::{oid_cast_to_u32, OID, XID};
use common::result::Result;

use crate::access::predicate::{Pred, PredCtx};
use crate::trans::tx_lock::{EmptyFlag, TxLock};

#[derive(Clone, Copy)]
pub enum LockAcquire {
    LockOk,
    LockFail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LockType {
    LockKeyRead,
    LockKeyWrite,
    LockPredicate,
}

pub struct LockInnerSet {
    read: HashSet<OID>,
    write: HashSet<OID>,
    predicate: HashSet<OID>,
    lock: HashMap<OID, Arc<TxLock>>,
    // lock oid -> TxLock
    wait: VecDeque<OID>, // waiting lock
}

pub struct LockSlotKey {
    removed: bool,
    inner: LockInnerSet,
}

pub struct LockSetPred<C: PredCtx, P: Pred<C>> {
    _ctx: PhantomData<C>,
    pred: P,
    tx_lock: Arc<TxLock>,
    keys: HashSet<Vec<u8>>,
}

impl LockSlotKey {
    pub fn new() -> Self {
        Self {
            removed: false,
            inner: LockInnerSet::new(),
        }
    }

    pub fn is_removed(&self) -> bool {
        self.removed
    }

    pub fn set_removed_flag(&mut self) {
        self.removed = true;
    }

    // this function only test if a lock can be acquired
    // waiting a lock out of this function depends on the return value
    pub async fn test_lock(&mut self, tx_lock: Arc<TxLock>) -> bool {
        trace!("test lock for {}", oid_cast_to_u32(tx_lock.xid()));
        let lock_ok = self.inner.lock_type(tx_lock.clone());
        if !lock_ok {
            let n = tx_lock.notifier();
            n.lock_increase_wait(1).await;
        }
        lock_ok
    }

    pub async fn add_lock(&mut self, oid: OID, lock_type: LockType) {
        self.inner.add_lock(oid, lock_type);
    }

    pub async fn unlock(&mut self, oid: OID) -> Option<EmptyFlag> {
        self.inner.unlock(oid).await
    }

    pub fn meet_predicate_lock(&mut self, key_lock_id: OID, tx_lock_predicate: Arc<TxLock>) {
        assert_eq!(tx_lock_predicate.lock_type(), LockType::LockPredicate);
        self.inner.meet_predicate(key_lock_id, tx_lock_predicate)
    }

    pub fn get_dependency(
        &mut self,
        lock_id: OID,
        xid: XID,
        in_dep_set: &mut HashSet<XID>,
    ) -> Result<()> {
        self.inner.get_dependency(lock_id, xid, in_dep_set)
    }
}

impl<C: PredCtx, P: Pred<C>> LockSetPred<C, P> {
    pub fn new(pred: P, tx_lock: Arc<TxLock>) -> Self {
        Self {
            _ctx: Default::default(),
            pred,
            tx_lock,
            keys: Default::default(),
        }
    }

    pub fn predicate(&self) -> &P {
        &self.pred
    }

    pub fn get_tx_lock(&self) -> Arc<TxLock> {
        self.tx_lock.clone()
    }

    pub fn meet_key(&mut self, key: Vec<u8>) {
        let _ = self.keys.insert(key);
    }

    pub fn get_conflict_key_set(&mut self) -> HashSet<Vec<u8>> {
        let mut key_set = HashSet::new();
        std::mem::swap(&mut self.keys, &mut key_set);
        key_set
    }
}

impl LockInnerSet {
    pub fn new() -> Self {
        Self {
            read: Default::default(),
            write: Default::default(),
            predicate: Default::default(),
            lock: Default::default(),
            wait: Default::default(),
        }
    }

    pub fn lock_type(&mut self, tx_lock: Arc<TxLock>) -> bool {
        self.lock_gut(tx_lock)
    }

    pub async fn unlock(&mut self, oid: OID) -> Option<EmptyFlag> {
        self.unlock_type(oid).await
    }

    pub async fn unlock_type(&mut self, oid: OID) -> Option<EmptyFlag> {
        let opt_lock = self.lock.remove(&oid);
        let tx_lock = match opt_lock {
            Some(l) => l,
            None => {
                return None;
            }
        };
        let lock_type = tx_lock.lock_type();
        let id = tx_lock.lock_id();
        let opt = self.unlock_gut(id, lock_type).await;
        opt
    }

    pub fn meet_predicate(&mut self, key_lock_id: OID, tx_lock: Arc<TxLock>) {
        let lock_type = tx_lock.lock_type();
        assert_eq!(lock_type, LockType::LockPredicate);
        let oid = tx_lock.lock_id();
        let _ = self.lock.insert(oid, tx_lock);
        self.add_wait_lock(key_lock_id)
    }

    pub fn lock_gut(&mut self, lock: Arc<TxLock>) -> bool {
        let oid = lock.lock_id();
        let xid = lock.xid();
        let lock_type = lock.lock_type();
        let _ = self.lock.insert(oid, lock.clone());

        return if self.wait.is_empty() {
            let no_conflict = self.is_no_conflict(xid, oid, lock_type);
            if !no_conflict {
                self.add_wait_lock(oid)
            } else {
                self.add_lock(oid, lock_type);
            }
            no_conflict
        } else {
            self.add_wait_lock(oid);
            false
        };
    }

    async fn unlock_gut(&mut self, oid: OID, lock_type: LockType) -> Option<EmptyFlag> {
        match lock_type {
            LockType::LockKeyRead => {
                let removed = self.read.remove(&oid);
                if removed && self.read.is_empty() {
                    self.try_notify_wait().await;
                }
            }
            LockType::LockPredicate => {
                let _removed = self.predicate.remove(&oid);
                // do not check if we remove success for predicate
                // the predicate can be not in this map
                self.try_notify_wait().await;
            }
            LockType::LockKeyWrite => {
                let removed = self.write.remove(&oid);
                if removed && self.write.is_empty() {
                    self.try_notify_wait().await;
                }
            }
        }

        let flag = EmptyFlag {
            is_read_row_empty: self.read.is_empty(),
            is_write_row_empty: self.write.is_empty(),
        };
        if flag.is_read_row_empty || flag.is_write_row_empty {
            Some(flag)
        } else {
            None
        }
    }

    fn add_wait_lock(&mut self, oid: XID) {
        self.wait.push_back(oid);
    }

    fn is_no_conflict(&self, xid: XID, oid: OID, lock_type: LockType) -> bool {
        match lock_type {
            LockType::LockKeyRead | LockType::LockPredicate => {
                self.write.is_empty() // write set is empty
                    || self.only_contain_this_tx(&self.write, xid, oid) // the writer is current transaction
            }
            LockType::LockKeyWrite => {
                (self.write.is_empty() || (self.only_contain_this_tx(&self.write, xid, oid)))
                    && (self.read.is_empty() || (self.only_contain_this_tx(&self.read, xid, oid)))
                    && (self.predicate.is_empty()
                        || (self.only_contain_this_tx(&self.predicate, xid, oid)))
            }
        }
    }

    fn only_contain_this_tx(&self, hash_set: &HashSet<OID>, current_xid: XID, oid: OID) -> bool {
        for i in hash_set {
            if *i != oid {
                let opt = self.lock.get(i);
                match opt {
                    None => {}
                    Some(o) => {
                        if o.lock_id() != current_xid {
                            return false;
                        }
                    }
                };
            }
        }
        return true;
    }
    fn add_lock(&mut self, oid: OID, lock_type: LockType) {
        match lock_type {
            LockType::LockKeyRead => {
                self.read.insert(oid);
            }
            LockType::LockKeyWrite => {
                self.write.insert(oid);
            }
            LockType::LockPredicate => {
                self.predicate.insert(oid);
            }
        }
    }

    pub async fn try_notify_wait(&mut self) {
        loop {
            let opt = self.wait.front();
            match opt {
                None => {
                    break;
                }
                Some(oid) => {
                    let opt = self.lock.get(oid);
                    let ok = match opt {
                        None => {
                            // for a canceled lock, this is possible
                            trace!("cannot find lock id, {}, lock set:", oid);
                            for (id, _) in &self.lock {
                                trace!("    lock id , {}", id);
                            }

                            return;
                        }
                        Some(o) => self.notify_one(o.xid(), o.lock_id(), o.lock_type()).await,
                    };
                    if ok {
                        self.wait.pop_front();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    async fn notify_one(&mut self, xid: XID, oid: OID, lock_type: LockType) -> bool {
        let opt = self.lock.get(&oid);
        let notifier = match opt {
            Some(l) => {
                if self.is_no_conflict(xid, oid, lock_type) {
                    l.notifier().clone()
                } else {
                    return false;
                }
            }
            None => {
                return true;
            }
        };
        self.add_lock(oid, lock_type);
        notifier.lock_notify().await
    }

    pub fn get_dependency(
        &mut self,
        lock_id: OID,
        xid: XID,
        in_dep_set: &mut HashSet<XID>,
    ) -> Result<()> {
        let opt = self.lock.get(&lock_id);
        let lock_type = match opt {
            Some(o) => o.lock_type(),
            None => {
                return Ok(());
            }
        };
        match lock_type {
            LockType::LockKeyRead | LockType::LockPredicate => {
                self.add_in_dep(xid, &self.write, in_dep_set);
            }
            LockType::LockKeyWrite => {
                for set in [&self.write, &self.predicate, &self.read] {
                    self.add_in_dep(xid, set, in_dep_set);
                }
            }
        }
        Ok(())
    }

    fn add_in_dep(&self, xid: XID, set: &HashSet<OID>, output: &mut HashSet<XID>) {
        for lock_id in set {
            match self.lock.get(lock_id) {
                Some(t) => {
                    let x = t.xid();
                    if x != xid {
                        output.insert(x);
                    }
                }
                None => {}
            }
        }
    }
}
