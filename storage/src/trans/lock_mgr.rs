use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_std::future::timeout;
use async_std::sync::Mutex;
use log::debug;
use tracing::{trace_span, Instrument};

use adt::slice::Slice;
use common::id::{OID, XID};
use common::result::Result;

use crate::access::bt_pred::BtPred;
use crate::access::predicate::{Pred, PredCtx, PredType};
use crate::trans::deadlock::DeadLockDetector;
use crate::trans::history::{History, TxHistOp};
use crate::trans::lock_slot::{LockSetPred, LockSlotKey, LockType};
use crate::trans::tx_lock::{KeyOrPred, TxLock};
use crate::trans::tx_wait_notifier::TxWaitNotifier;

pub struct LockMgr<C: PredCtx + Send + 'static, P: Pred<C> + Send + 'static, H: History> {
    table_id: OID,
    deadlock: Arc<DeadLockDetector>,
    keys: scc::HashMap<Vec<u8>, Arc<Mutex<LockSlotKey>>>,
    predicates: scc::HashMap<OID, Arc<Mutex<LockSetPred<C, P>>>>,
    duration_detect: Duration,
    history: H,
}

impl<C: PredCtx + Send + 'static, P: Pred<C> + Send + 'static, H: History> LockMgr<C, P, H> {
    pub fn new(oid: OID, deadlock: Arc<DeadLockDetector>, history: H) -> Self {
        let duration_detect = deadlock.duration_detect();
        Self {
            table_id: oid,
            deadlock,
            keys: Default::default(),
            predicates: Default::default(),
            duration_detect,
            history,
        }
    }

    #[allow(dead_code)]
    pub fn table_id(&self) -> OID {
        self.table_id
    }

    // How to use a predicate lock
    // 1. call lock_predicate_init when first initialize this lock
    // 2. if predicate encounter a conflict key lock,
    //      call lock_predicate_step
    //
    pub async fn lock_predicate_init(
        &self,
        oid: OID,
        notifier: Arc<TxWaitNotifier>,
        pred: &P,
    ) -> Result<Arc<TxLock>> {
        let tx_lock = self
            .predicate_first(oid, notifier, pred)
            .instrument(trace_span!("predicate_first"))
            .await;

        Ok(tx_lock)
    }

    // How to use a key lock
    // 1. call lock_key_init when first initialize this lock
    // 2. if predicate encounter a conflict predicate lock,
    //      call lock_key_step
    pub async fn lock_key_init<K: Slice>(
        &self,
        oid: OID,
        notifier: Arc<TxWaitNotifier>,
        l_type: LockType,
        key: &K,
    ) -> Result<Arc<TxLock>> {
        assert_ne!(l_type, LockType::LockPredicate);
        let (ok, tx_lock) = self.key_first(oid, notifier.clone(), l_type, key).await;
        let ret = if ok {
            Ok(tx_lock)
        } else {
            self.tx_lock_wait(key, tx_lock.lock_id(), tx_lock.notifier())
                .instrument(trace_span!("lock key, tx_lock_wait"))
                .await?;
            Ok(tx_lock)
        };
        self.append_history_row_operation(notifier.xid(), oid, self.table_id, key, l_type)
            .await;
        ret
    }

    pub async fn lock_predicate_step<K: Slice>(&self, tx_lock: Arc<TxLock>, key: &K) -> Result<()> {
        if !self.predicate_meet_key(tx_lock.clone(), key).await {
            self.tx_lock_wait(key, tx_lock.lock_id(), tx_lock.notifier())
                .instrument(trace_span!("lock predicate, tx_lock_wait"))
                .await?;
        }
        self.history
            .append(
                tx_lock.xid(),
                TxHistOp::THRange(self.table_id, tx_lock.lock_id(), None),
            )
            .await;
        Ok(())
    }

    pub async fn unlock(&self, tx_lock: Arc<TxLock>) {
        let lock_type = tx_lock.lock_type();
        match lock_type {
            LockType::LockPredicate => {
                self.unlock_predicate(tx_lock).await;
            }
            _ => {
                self.unlock_key(tx_lock).await;
            }
        }
    }
    pub async fn lock_key_step<K: Slice>(
        &self,
        key: &K,
        tx_lock: Arc<TxLock>,
        pred_oid: OID,
    ) -> Result<()> {
        if self
            .key_meet_predicate(key, tx_lock.clone(), pred_oid)
            .await
        {
            Ok(())
        } else {
            self.tx_lock_wait(key, tx_lock.lock_id(), tx_lock.notifier())
                .instrument(trace_span!("lock key, tx_lock_wait"))
                .await
        }
    }

    // create a new predicate lock and add it to lock table
    // always success
    async fn predicate_first(
        &self,
        oid: OID,
        notifier: Arc<TxWaitNotifier>,
        pred: &P,
    ) -> Arc<TxLock> {
        let tx_lock = Arc::new(TxLock::new(
            oid,
            self.table_id,
            LockType::LockPredicate,
            notifier,
            KeyOrPred::Pred(Box::new(pred.clone())),
        ));
        {
            let pl = LockSetPred::new(pred.clone(), tx_lock.clone());
            let r = self
                .predicates
                .insert_async(oid, Arc::new(Mutex::new(pl)))
                .await;
            assert!(r.is_ok());
            if r.is_err() {
                panic!("fatal error");
            }
        };

        return tx_lock.clone();
    }

    async fn predicate_meet_key<K: Slice>(&self, tx_lock: Arc<TxLock>, key: &K) -> bool {
        let key_lock_slot = {
            match self.get_key_lock_slot(key).await {
                Some(k) => k,
                None => {
                    return true;
                }
            }
        };
        {
            let pred = {
                let oid = tx_lock.lock_id();
                let opt = self.predicates.read_async(&oid, |_, v| v.clone()).await;
                match opt {
                    Some(l) => l.clone(),
                    None => {
                        return true;
                    } // no such predicate lock ...
                }
            };
            {
                let mut guard = pred.lock().await;
                guard.meet_key(Vec::from(key.as_slice()));
            }
        }
        {
            let mut guard = key_lock_slot.lock().await;
            guard.add_lock(tx_lock.lock_id(), tx_lock.lock_type()).await;
            guard.test_lock(tx_lock).await
        }
    }

    async fn key_first<K: Slice>(
        &self,
        oid: OID,
        notifier: Arc<TxWaitNotifier>,
        l_type: LockType,
        key: &K,
    ) -> (bool, Arc<TxLock>) {
        let tx_lock = Arc::new(TxLock::new(
            oid,
            self.table_id,
            l_type,
            notifier,
            KeyOrPred::Key(Vec::from(key.as_slice())),
        ));
        loop {
            // loop until install lock

            let opt = self.keys.read_async(key.as_slice(), |_, v| v.clone()).await;

            match opt {
                None => {
                    let mut key_lock = LockSlotKey::new();
                    let lock_ok = key_lock.test_lock(tx_lock.clone()).await;
                    let result = self
                        .keys
                        .insert_async(Vec::from(key.as_slice()), Arc::new(Mutex::new(key_lock)))
                        .await;
                    if result.is_ok() {
                        // insert ok
                        return (lock_ok, tx_lock);
                    }
                    // or else there is a equal key, continue loop to read it
                }
                Some(l) => {
                    let mut guard = l.lock().await;
                    if !guard.is_removed() {
                        // Is this lock has not been removed
                        return (guard.test_lock(tx_lock.clone()).await, tx_lock);
                    }
                    // or else the key is removed, continue loop to create a new lock slot
                }
            };
        }
    }

    async fn key_meet_predicate<K: Slice>(
        &self,
        key: &K,
        tx_lock: Arc<TxLock>,
        pred_oid: OID,
    ) -> bool {
        let pred_lock_slot = {
            let opt = self
                .predicates
                .read_async(&pred_oid, |_, v| v.clone())
                .await;
            match opt {
                Some(l) => l.clone(),
                None => {
                    // no such predicate lock ...
                    return false;
                }
            }
        };
        let tx_pred_lock = {
            let mut guard = pred_lock_slot.lock().await;
            guard.meet_key(Vec::from(key.as_slice()));
            guard.get_tx_lock()
        };
        let key_lock_slot = {
            match self.get_key_lock_slot(key).await {
                Some(k) => k,
                None => {
                    return true;
                }
            }
        };
        {
            let mut guard = key_lock_slot.lock().await;
            guard.meet_predicate_lock(tx_lock.lock_id(), tx_pred_lock);
            guard.test_lock(tx_lock).await
        }
    }

    async fn unlock_key(&self, tx_lock: Arc<TxLock>) {
        let key = match tx_lock.key() {
            Some(key) => key,
            _ => {
                return;
            }
        };
        let oid = tx_lock.lock_id();
        let lock_slot = {
            let opt = self.keys.read_async(key.as_slice(), |_, v| v.clone()).await;
            match opt {
                Some(v) => v.clone(),
                None => {
                    return;
                }
            }
        };

        let mut guard = lock_slot.lock().await;
        let opt_flag = guard.unlock(oid).await;
        match opt_flag {
            Some(f) => {
                if f.is_empty() {
                    // No read or write locks on this key, so we can remove this key safely.
                    // Set the removed flag first, to forbid later coming transaction to install any
                    // new locks on this key
                    guard.set_removed_flag();
                    let _ = self.keys.remove_async(key).await;
                }
            }
            None => {}
        }
    }

    async fn unlock_predicate(&self, tx_lock: Arc<TxLock>) {
        let lock_id = tx_lock.lock_id();
        let opt = { self.predicates.remove_async(&lock_id).await };
        let conflict_key_set = match opt {
            Some((_id, l)) => {
                let mut guard = l.lock().await;
                guard.get_conflict_key_set()
            }
            None => {
                return;
            }
        };

        for key in &conflict_key_set {
            let lock_slot = {
                let opt = self.keys.read_async(key.as_slice(), |_, v| v.clone()).await;
                match opt {
                    Some(v) => v.clone(),
                    None => {
                        continue;
                    }
                }
            };
            {
                let mut guard = lock_slot.lock().await;
                let _ = guard.unlock(lock_id).await;
            }
        }
    }

    async fn tx_lock_wait<K: Slice>(
        &self,
        key: &K,
        oid: OID,
        notifier: Arc<TxWaitNotifier>,
    ) -> Result<()> {
        let xid = notifier.xid();
        let mut in_dep_set = HashSet::new();
        loop {
            debug!("lock wait, {}", oid);
            let f = notifier.lock_wait_lock();
            let result_timeout = timeout(self.duration_detect, f)
                .instrument(trace_span!("wait timeout"))
                .await;
            match result_timeout {
                Ok(result_wait) => {
                    debug!("lock wait, {} end", oid);
                    return result_wait;
                }
                Err(_) => {
                    self.send_new_dependency(oid, xid, key, &mut in_dep_set)
                        .await?;
                    for in_xid in &in_dep_set {
                        self.deadlock.add_dependency(*in_xid, xid).await?;
                    }
                }
            }
        }
    }

    async fn send_new_dependency<K: Slice>(
        &self,
        lock_id: OID,
        xid: OID,
        key: &K,
        in_dep_set: &mut HashSet<XID>,
    ) -> Result<()> {
        let lock_slot = {
            let opt = self.keys.read_async(key.as_slice(), |_, v| v.clone()).await;
            match opt {
                Some(v) => v.clone(),
                None => {
                    return Ok(());
                }
            }
        };
        {
            let mut guard = lock_slot.lock().await;
            guard.get_dependency(lock_id, xid, in_dep_set)?;
        }
        Ok(())
    }

    async fn get_key_lock_slot<K: Slice>(&self, key: &K) -> Option<Arc<Mutex<LockSlotKey>>> {
        let opt = self.keys.read_async(key.as_slice(), |_, v| v.clone()).await;
        match opt {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }

    // fixme debug tx history
    #[allow(dead_code)]
    async fn append_history_pred_operation(&self, xid: XID, oid: OID, opt_pred: Option<&P>) {
        let opt_range = match opt_pred {
            Some(pred) => match P::pred_type() {
                PredType::PredRange => {
                    let range = unsafe {
                        let bt = std::mem::transmute::<&P, &BtPred>(pred);
                        bt.range().clone()
                    };
                    Some(range)
                }
                _ => {
                    panic!("TODO");
                }
            },
            None => None,
        };
        self.history
            .append(xid, TxHistOp::THRange(self.table_id, oid, opt_range))
            .await;
    }

    async fn append_history_row_operation<K: Slice>(
        &self,
        xid: XID,
        lock_id: OID,
        table_id: OID,
        key: &K,
        l_type: LockType,
    ) {
        let k_vec = Vec::from(key.as_slice());
        match l_type {
            LockType::LockKeyRead => {
                self.history
                    .append(xid, TxHistOp::THRead(table_id, lock_id, k_vec))
                    .await;
            }
            LockType::LockKeyWrite => {
                self.history
                    .append(xid, TxHistOp::THWrite(table_id, lock_id, k_vec))
                    .await;
            }
            _ => {}
        }
    }
}
