use std::sync::Arc;
use std::time::Duration;

use scc::HashMap;

use adt::compare::Compare;
use adt::range::RangeBounds;
use adt::slice::Slice;
use common::error_type::ET;
use common::id::OID;
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, UpdateOption};

use crate::access::buf_pool::BufPool;
use crate::access::handle_read::HandleReadKV;
use crate::access::handle_write::HandleWrite;
use crate::access::schema::TableSchema;
use crate::access::to_json::ToJson;

use crate::bt_table_handler::BTreeHandler;
use crate::d_type::data_type::DataType;
use crate::d_type::data_type_func::{map_name_to_data_type, DataTypeInit};
use crate::table_context::TableContext;
use crate::trans::deadlock::DeadLockDetector;
use crate::trans::history::{History, TxHistOp};
use crate::trans::log_cmd::LogCmd;
use crate::trans::redo_log::RedoLog;
use crate::trans::tx_context::{ReleaseType, TxContext};
use crate::trans::tx_lock::TxLock;
use crate::trans::tx_state::TxState;

pub struct StorageHandler<
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    HI: History + 'static,
> {
    history: HI,

    buffer_pool: Arc<BufPool>,
    deadlock_detector: Arc<DeadLockDetector>,
    redo_log: Arc<RedoLog>,
    type_init: std::collections::HashMap<DataType, DataTypeInit>,
    schema_manager: HashMap<String, TableSchema>,
    table_context: HashMap<OID, Arc<TableContext<C, KT, VT>>>,
    btree_tables: HashMap<OID, Arc<BTreeHandler<C, KT, VT, HI>>>,
    enable_debug_latch: bool,
}

unsafe impl<
        C: Compare<[u8]> + 'static,
        KT: ToJson + 'static,
        VT: ToJson + 'static,
        HI: History + 'static,
    > Send for StorageHandler<C, KT, VT, HI>
{
}

unsafe impl<
        C: Compare<[u8]> + 'static,
        KT: ToJson + 'static,
        VT: ToJson + 'static,
        HI: History + 'static,
    > Sync for StorageHandler<C, KT, VT, HI>
{
}

impl<C: Compare<[u8]> + 'static, KT: ToJson + 'static, VT: ToJson + 'static, HI: History>
    StorageHandler<C, KT, VT, HI>
{
    pub fn new(buff_size: u64, page_size: u64, path: String, history: HI) -> Self {
        let buffer_pool = Arc::new(BufPool::new(buff_size, page_size, path.clone()));
        let redo_log = Arc::new(RedoLog::new(path.clone()));

        let deadlock_detector = Arc::new(DeadLockDetector::new(Duration::from_millis(500)));
        let type_init = map_name_to_data_type();
        Self {
            history,
            buffer_pool,
            deadlock_detector,
            redo_log,
            type_init,
            schema_manager: Default::default(),
            table_context: HashMap::default(),
            btree_tables: Default::default(),
            enable_debug_latch: false,
        }
    }

    pub fn deadlock_detector(&self) -> &Arc<DeadLockDetector> {
        &self.deadlock_detector
    }

    pub fn redo_log(&self) -> &Arc<RedoLog> {
        &self.redo_log
    }

    pub fn get_type_init(&self) -> &std::collections::HashMap<DataType, DataTypeInit> {
        &self.type_init
    }
    pub async fn create_table(
        &self,
        schema: TableSchema,
        ctx: TableContext<C, KT, VT>,
    ) -> Result<()> {
        let table_name = schema.name.clone();
        let opt2 = self
            .table_context
            .insert_async(schema.table_id, Arc::new(ctx))
            .await;
        match opt2 {
            Ok(_) => {}
            Err(_) => {
                return Err(ET::ExistingSuchElement);
            }
        };

        let opt = self
            .schema_manager
            .insert_async(table_name.clone(), schema)
            .await;
        match opt {
            Err(_) => {
                return Err(ET::ExistingSuchElement);
            }
            Ok(_) => {}
        }
        self.open(&table_name).await?;
        Ok(())
    }

    pub async fn open(&self, table: &String) -> Result<OID> {
        let r = self
            .schema_manager
            .read_async(table, |_, s| (s.table_id, s.index_id, s.heap_id))
            .await;
        let (table_id, index_id, heap_id) = match r {
            Some(id) => id,
            None => {
                return Err(ET::NoSuchElement);
            }
        };

        if self.btree_tables.contains_async(&table_id).await {
            return Ok(table_id);
        }

        let opt_desc = self
            .table_context
            .read_async(&table_id, |_, ctx| {
                (
                    ctx.compare.clone(),
                    ctx.key2json.clone(),
                    ctx.value2json.clone(),
                )
            })
            .await;
        match opt_desc {
            Some((key_cmp, key2json, v2json)) => {
                let handler = Arc::new(
                    BTreeHandler::new(
                        table_id,
                        self.redo_log.clone(),
                        key_cmp,
                        key2json,
                        v2json,
                        index_id,
                        heap_id,
                        self.buffer_pool.clone(),
                        self.deadlock_detector.clone(),
                        self.history.clone(),
                    )
                    .enable_debug_latch(self.enable_debug_latch),
                );
                handler.open().await?;
                let _ = self.btree_tables.insert_async(table_id, handler).await;
            }
            None => {
                return Err(ET::NoSuchElement);
            }
        }

        return Ok(table_id);
    }

    pub async fn search_key<K, F>(
        &self,
        context: &TxContext,
        oid: OID,
        key: &K,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let table = self.get_table_handler(oid).await?;
        table.search_key(context, key, opt, handler).await?;
        Ok(())
    }

    /// range search
    pub async fn search_range<K, F>(
        &self,
        context: &TxContext,
        oid: OID,
        range: &RangeBounds<K>,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>
    where
        K: Slice,
        F: HandleReadKV,
    {
        //debug!("tx {} search range table {}",context.xid(),oid );
        let table = self.get_table_handler(oid).await?;
        table.search_range(context, range, opt, handler).await?;
        Ok(())
    }

    pub async fn insert<K: Slice, V: Slice, H: HandleWrite>(
        &self,
        context: &TxContext,
        oid: OID,
        key: &K,
        value: &V,
        opt: InsertOption,
        handler: &H,
    ) -> Result<()> {
        let table = self.get_table_handler(oid).await?;
        table.tx_insert(context, key, value, handler, opt).await?;
        Ok(())
    }

    /// update a key/value pair
    pub async fn update<K, V, H>(
        &self,
        context: &TxContext,
        oid: OID,
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
        let table = self.get_table_handler(oid).await?;
        table.tx_update(context, key, value, opt, handler).await?;

        Ok(())
    }

    /// update a key/value pair
    pub async fn delete<K, HW>(
        &self,
        context: &TxContext,
        oid: OID,
        key: &K,
        opt: DeleteOption,
        handler: &HW,
    ) -> Result<()>
    where
        K: Slice,
        HW: HandleWrite,
    {
        let table = self.get_table_handler(oid).await?;
        table.tx_delete(context, key, opt, handler).await?;
        Ok(())
    }

    pub async fn begin_tx(&self, context: &TxContext) -> Result<()> {
        let log = LogCmd::LogBeginTx(context.xid());
        self.redo_log.append_log(log, None).await;
        Ok(())
    }

    pub async fn commit_tx(&self, context: &TxContext) -> Result<()> {
        context
            .release_lock(ReleaseType::ReleaseRead, &self)
            .await?;
        context.notifier().set_state(TxState::TxCommitted).await;
        self.history.append(context.xid(), TxHistOp::THCommit).await;
        self.deadlock_detector.remove(context.xid()).await?;
        let log = LogCmd::LogCommitTx(context.xid());
        self.redo_log
            .append_log(log, Some(context.notifier()))
            .await;
        context.notifier().log_wait_flush().await?;

        let ops = context.get_write_op().await;
        for op in ops {
            let table = op.table_id();
            let handler = self.get_table_handler(table).await?;
            handler.write_operation(&op).await?;
        }
        context
            .release_lock(ReleaseType::ReleaseWrite, &self)
            .await?;
        // TODO release lock
        Ok(())
    }

    pub async fn abort_tx(&self, context: &TxContext) -> Result<()> {
        context.release_lock(ReleaseType::ReleaseAll, &self).await?;
        context.notifier().set_state(TxState::TxAborted).await;
        self.history.append(context.xid(), TxHistOp::THAbort).await;
        self.deadlock_detector.remove(context.xid()).await?;
        let log = LogCmd::LogAbortTx(context.xid());
        self.redo_log
            .append_log(log, Some(context.notifier()))
            .await;
        context.notifier().log_wait_flush().await?;
        // TODO release lock
        Ok(())
    }

    pub async fn get_table_handler(&self, oid: OID) -> Result<Arc<BTreeHandler<C, KT, VT, HI>>> {
        let opt_handler = self.btree_tables.read_async(&oid, |_k, v| v.clone()).await;
        return match opt_handler {
            None => Err(ET::NoSuchElement),
            Some(h) => Ok(h),
        };
    }
    pub async fn get_table_context_by_name(
        &self,
        name: &String,
    ) -> Result<Arc<TableContext<C, KT, VT>>> {
        let opt_oid = self
            .schema_manager
            .read_async(name, |_, v| v.table_id)
            .await;
        let oid = match opt_oid {
            Some(id) => id,
            None => {
                return Err(ET::NoSuchElement);
            }
        };
        self.get_table_context(oid).await
    }

    pub async fn get_table_context(&self, oid: OID) -> Result<Arc<TableContext<C, KT, VT>>> {
        let opt_context = self.table_context.read_async(&oid, |_k, v| v.clone()).await;
        return match opt_context {
            None => Err(ET::NoSuchElement),
            Some(h) => Ok(h),
        };
    }
    pub async fn release_lock(&self, tx_lock: Arc<TxLock>) -> Result<()> {
        let handler = self.get_table_handler(tx_lock.table_id()).await?;

        handler.lock_mgr().unlock(tx_lock.clone()).await;
        let opt = tx_lock.get_opt_flag();
        match opt {
            Some(f) => match tx_lock.key() {
                Some(key) => {
                    handler
                        .remove_lock(key, f.is_read_empty(), f.is_write_empty(), None)
                        .await?
                }
                None => {}
            },
            None => {}
        }
        Ok(())
    }

    pub async fn debug_assert(&self) {}
}
