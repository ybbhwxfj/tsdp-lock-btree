use std::sync::Arc;

use tracing::{trace_span, Instrument};

use adt::compare::Compare;
use adt::range::RangeBounds;
use adt::slice::Slice;
use common::error_type::ET;
use common::id::{gen_oid, OID};
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, TxOption, UpdateOption};
use crate::access::bt_pred::{BtPred, BtPredCtx};
use crate::access::btree_index_tx::{BTreeIndexTx, TxProgress};
use crate::access::buf_pool::BufPool;
use crate::access::cursor::Cursor;
use crate::access::file_id::FileId;
use crate::access::handle_read::HandleReadKV;
use crate::access::handle_write::{EmptyHandleWrite, HandleWrite};
use crate::access::to_json::ToJson;
use crate::trans::deadlock::DeadLockDetector;
use crate::trans::history::History;
use crate::trans::lock_mgr::LockMgr;
use crate::trans::lock_slot::LockType;
use crate::trans::log_cmd::LogCmd;
use crate::trans::redo_log::RedoLog;
use crate::trans::tx_context::TxContext;
use crate::trans::tx_lock::TxLock;
use crate::trans::write_operation::{WriteOp, WriteOpType};

pub struct BTreeHandler<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, HI: History> {
    table_id: OID,
    redo_log: Arc<RedoLog>,
    lock_mgr: Arc<LockMgr<BtPredCtx<C>, BtPred, HI>>,
    index: BTreeIndexTx<C, KT, VT>,
}

impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, HI: History> BTreeHandler<C, KT, VT, HI> {
    pub fn new(
        table_id: OID,
        redo_log: Arc<RedoLog>,
        key_cmp: C,
        key2json: KT,
        value2json: VT,
        btree_file: FileId,
        heap_file: FileId,
        pool: Arc<BufPool>,
        dl: Arc<DeadLockDetector>,
        history: HI,
    ) -> Self {
        Self {
            table_id,
            redo_log,
            lock_mgr: Arc::new(LockMgr::new(table_id, dl, history)),
            index: BTreeIndexTx::new(
                key_cmp,
                key2json,
                value2json,
                btree_file,
                heap_file,
                gen_oid(), // TODO ... old version storage
                pool,
            ),
        }
    }

    pub fn enable_debug_latch(self, enable_debug_latch: bool) -> Self {
        let mut this = self;
        this.index = this.index.enable_debug_latch(enable_debug_latch);
        this
    }

    pub fn index(&self) -> &BTreeIndexTx<C, KT, VT> {
        &self.index
    }

    pub async fn open(&self) -> Result<()> {
        self.index.open().await
    }

    pub fn lock_mgr(&self) -> &Arc<LockMgr<BtPredCtx<C>, BtPred, HI>> {
        &self.lock_mgr
    }

    pub async fn search_key<K, F>(
        &self,
        context: &TxContext,
        key: &K,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let tx = context;
        let lock = self
            .lock_mgr
            .lock_key_init(gen_oid(), tx.notifier().clone(), LockType::LockKeyRead, key)
            .await?;
        tx.add_lock(lock.clone()).await;
        let mut tx_opt = Some(TxOption::new());
        loop {
            let ret = self
                .index
                .search_key(key, opt.clone(), handler, &mut tx_opt)
                .await;
            let ok = self
                .access_key_test_result(ret, key, lock.clone(), &mut tx_opt)
                .await?;
            if ok {
                break;
            }
        }
        Ok(())
    }

    /// range search
    pub async fn search_range<K, F>(
        &self,
        context: &TxContext,
        range: &RangeBounds<K>,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let tx = context;
        let lock = self
            .lock_mgr
            .lock_predicate_init(gen_oid(), tx.notifier().clone(), &BtPred::from(range))
            .await?;
        tx.add_lock(lock.clone()).await;
        let mut tx_opt = Some(TxOption::new());
        loop {
            let ret = self
                .index
                .search_range(range, opt.clone(), handler, &mut tx_opt)
                .instrument(trace_span!("index.insert"))
                .await;
            let ok = self
                .access_range_test_result(ret, lock.clone(), &mut tx_opt)
                .await?;
            if ok {
                break;
            }
        }
        return Ok(());
    }

    pub async fn tx_insert<K: Slice, V: Slice, H: HandleWrite>(
        &self,
        context: &TxContext,
        key: &K,
        value: &V,
        handler: &H,
        opt: InsertOption,
    ) -> Result<()> {
        let tx = context;
        let write_op = WriteOp::new_insert(
            self.table_id,
            Vec::from(key.as_slice()),
            Vec::from(value.as_slice()),
        );
        tx.add_write_op(write_op.clone()).await;
        let log = LogCmd::LogWriteOp(context.xid(), write_op);
        self.redo_log.append_log(log, None).await;
        let lock = self
            .lock_mgr
            .lock_key_init(gen_oid(), tx.notifier().clone(), LockType::LockKeyRead, key)
            .await?;
        tx.add_lock(lock.clone()).await;
        let mut tx_opt = Some(TxOption::new());
        loop {
            let ret = self
                .index
                .insert::<_, _, _, { TxProgress::TxInProgress }>(
                    key,
                    value,
                    opt.clone(),
                    handler,
                    &mut tx_opt,
                )
                .instrument(trace_span!("index.insert"))
                .await;

            let ok = self
                .access_key_test_result(ret, key, lock.clone(), &mut tx_opt)
                .await?;
            if ok {
                break;
            }
        }
        return Ok(());
    }

    /// update a key/value pair
    pub async fn tx_update<K, V, H>(
        &self,
        context: &TxContext,
        key: &K,
        value: &V,
        opt: UpdateOption,
        handler: &H,
    ) -> Result<()>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        let tx = context;
        let write_op = WriteOp::new_update(
            self.table_id,
            Vec::from(key.as_slice()),
            Vec::from(value.as_slice()),
        );
        tx.add_write_op(write_op.clone()).await;
        let log = LogCmd::LogWriteOp(context.xid(), write_op);
        self.redo_log.append_log(log, None).await;

        let lock = self
            .lock_mgr
            .lock_key_init(gen_oid(), tx.notifier().clone(), LockType::LockKeyRead, key)
            .await?;
        tx.add_lock(lock.clone()).await;
        let mut tx_opt = Some(TxOption::new());
        loop {
            let ret = self
                .index
                .update::<_, _, _, { TxProgress::TxInProgress }>(
                    key,
                    value,
                    opt.clone(),
                    handler,
                    &mut tx_opt,
                )
                .instrument(trace_span!("index.update"))
                .await;
            let ok = self
                .access_key_test_result(ret, key, lock.clone(), &mut tx_opt)
                .await?;
            if ok {
                break;
            }
        }
        return Ok(());
    }

    /// delete a key/value pair
    pub async fn tx_delete<K, H>(
        &self,
        context: &TxContext,
        key: &K,
        opt: DeleteOption,
        handler: &H,
    ) -> Result<()>
    where
        K: Slice,
        H: HandleWrite,
    {
        let tx = context;
        let write_op = WriteOp::new_delete(self.table_id, Vec::from(key.as_slice()));
        tx.add_write_op(write_op.clone()).await;
        let log = LogCmd::LogWriteOp(context.xid(), write_op);
        self.redo_log.append_log(log, None).await;

        let lock = self
            .lock_mgr
            .lock_key_init(gen_oid(), tx.notifier().clone(), LockType::LockKeyRead, key)
            .instrument(trace_span!("lock_key_init"))
            .await?;
        tx.add_lock(lock.clone()).await;
        let mut tx_opt = Some(TxOption::new());
        loop {
            let ret = self
                .index
                .delete::<_, _, { TxProgress::TxInProgress }>(
                    key,
                    opt.clone(),
                    handler,
                    &mut tx_opt,
                )
                .instrument(trace_span!("index.delete"))
                .await;
            let ok = self
                .access_key_test_result(ret, key, lock.clone(), &mut tx_opt)
                .await?;
            if ok {
                break;
            }
        }
        return Ok(());
    }

    pub async fn write_operation(&self, op: &WriteOp) -> Result<()> {
        if self.table_id != op.table_id() {
            panic!("error");
        }

        match op.op_type() {
            WriteOpType::Update(v) | WriteOpType::Insert(v) => {
                let mut opt = UpdateOption::default();
                opt.insert_if_not_exist = true;
                let ok = self
                    .index
                    .update::<_, _, _, { TxProgress::TxCommitted }>(
                        op.key(),
                        v,
                        opt,
                        &EmptyHandleWrite::default(),
                        &mut None,
                    )
                    .await?;
                if !ok {
                    panic!("error");
                }
            }
            WriteOpType::Delete => {
                let mut opt = DeleteOption::default();
                opt.remove_key = true;
                let ok = self
                    .index
                    .delete::<_, _, { TxProgress::TxCommitted }>(
                        op.key(),
                        opt,
                        &EmptyHandleWrite::default(),
                        &mut None,
                    )
                    .await?;
                if !ok {
                    panic!("error");
                }
            }
            WriteOpType::UpdateDelta(_delta) => {}
        }
        Ok(())
    }

    pub async fn remove_lock<K: Slice>(
        &self,
        key: &K,
        read: bool,
        write: bool,
        opt_cursor: Option<Cursor>,
    ) -> Result<()>
    where
        K: Slice,
    {
        self.index.remove_lock(key, read, write, opt_cursor).await
    }

    async fn access_range_test_result<T>(
        &self,
        result: Result<T>,
        tx_lock: Arc<TxLock>,
        option: &mut Option<TxOption>,
    ) -> Result<bool> {
        let e = match result {
            Ok(_) => {
                return Ok(true);
            }
            Err(e) => e,
        };
        let tx_opt = match option {
            None => {
                return Err(ET::NoneOption);
            }
            Some(o) => o,
        };
        let _ = tx_opt.install_set();
        if e == ET::TxConflict {
            for key in tx_opt.get_conflict_key() {
                self.lock_mgr
                    .lock_predicate_step(tx_lock.clone(), key)
                    .await?;
            }
            tx_opt.add_conflict_to_checked();
            Ok(false)
        } else {
            Err(e)
        }
    }

    async fn access_key_test_result<K: Slice, T>(
        &self,
        result: Result<T>,
        key: &K,
        tx_lock: Arc<TxLock>,
        option: &mut Option<TxOption>,
    ) -> Result<bool> {
        let e = match result {
            Ok(_) => {
                return Ok(true);
            }
            Err(e) => e,
        };

        let tx_opt = match option {
            None => {
                return Err(ET::NoneOption);
            }
            Some(o) => o,
        };
        let _ = tx_opt.install_set();
        if e == ET::TxConflict {
            for oid in tx_opt.get_conflict_predicate() {
                self.lock_mgr
                    .lock_key_step(key, tx_lock.clone(), *oid)
                    .await?;
            }
            tx_opt.add_conflict_to_checked();
            Ok(false)
        } else {
            Err(e)
        }
    }
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, HI: History> Send
    for BTreeHandler<C, KT, VT, HI>
{
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, HI: History> Sync
    for BTreeHandler<C, KT, VT, HI>
{
}
