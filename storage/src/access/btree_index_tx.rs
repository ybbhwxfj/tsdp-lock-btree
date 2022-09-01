/// non-cluster btree index
/// non-leaf page stores the pointer(page_id, slot_no) to a tuple in a heap file
///
use std::sync::Arc;

use async_trait::async_trait;
use json::{object, JsonValue};
use tokio::sync::Mutex;
use tracing::{error, trace_span, Instrument};

use adt::compare::Compare;
use adt::range::RangeBounds;
use adt::slice::Slice;
use common::error_type::ET;
use common::id::{gen_oid, OID};
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, TxOption, UpdateOption};
use crate::access::btree_file::{BTreeFile, IsCluster};
use crate::access::buf_pool::BufPool;
use crate::access::const_val::INVALID_PAGE_ID;
use crate::access::cursor::Cursor;
use crate::access::file_id::FileId;
use crate::access::handle_read::{HandleReadKV, HandleReadTuple};
use crate::access::handle_write::HandleWrite;
use crate::access::heap_file::HeapFile;
use crate::access::slot_pointer::{SlotPointMutRef, SlotPointer, SlotPointerRef};
use crate::access::to_json::ToJson;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum TxProgress {
    TxCommitted,
    TxInProgress,
}

pub struct BTreeIndexTx<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson> {
    btree_file: BTreeFile<C, KT, Pointer2Json, { IsCluster::NonCluster }>,
    heap_file_committed: HeapFile<VT>,
    heap_file_old_version: HeapFile<VT>,
}

fn pointer_to_slot(s: &[u8]) -> SlotPointerRef {
    SlotPointerRef::from(s)
}

#[allow(dead_code)]
fn pointer_to_slot_mut(s: &mut [u8]) -> SlotPointMutRef {
    SlotPointMutRef::from(s)
}

impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson> BTreeIndexTx<C, KT, VT> {
    pub fn new(
        key_cmp: C,
        key2json: KT,
        value2json: VT,
        btree_file: FileId,
        heap_file_c: FileId,
        heap_file_old: FileId,
        pool: Arc<BufPool>,
    ) -> Self {
        let btree_file = BTreeFile::new(
            key_cmp,
            key2json,
            Pointer2Json::default(),
            btree_file,
            pool.clone(),
        );
        let heap_file_committed = HeapFile::new(heap_file_c, value2json.clone(), pool.clone());
        let heap_file_old_version = HeapFile::new(heap_file_old, value2json.clone(), pool.clone());
        Self {
            btree_file,
            heap_file_committed,
            heap_file_old_version,
        }
    }
    pub fn enable_debug_latch(self, enable_debug_latch: bool) -> Self {
        let mut this = self;
        this.btree_file = this.btree_file.enable_debug_latch(enable_debug_latch);
        this
    }
    pub async fn open(&self) -> Result<()> {
        self.btree_file.open().await?;
        self.heap_file_committed.open().await?;
        self.heap_file_old_version.open().await?;
        Ok(())
    }

    pub async fn search_key<K, F>(
        &self,
        key: &K,
        opt: SearchOption,
        handler: &F,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let handle_nc_heap = HandleReadNCVHeap::new(handler);
        let handle_nc_bt = HandleReadNCVBTree::<_, _, { TxProgress::TxInProgress }>::new(
            &self.heap_file_committed,
            &self.heap_file_old_version,
            handle_nc_heap,
        );
        let r = self
            .btree_file
            .search_key(key, opt, &handle_nc_bt, tx_opt)
            .instrument(trace_span!("btree file search key"))
            .await;
        self.debug_check_result(r).await
    }

    /// range search
    pub async fn search_range<K, F>(
        &self,
        range: &RangeBounds<K>,
        opt: SearchOption,
        handle_read: &F,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let handle_nc_heap = HandleReadNCVHeap::new(handle_read);
        let handle_nc_bt = HandleReadNCVBTree::<_, _, { TxProgress::TxCommitted }>::new(
            &self.heap_file_committed,
            &self.heap_file_old_version,
            handle_nc_heap,
        );
        let r = self
            .btree_file
            .search_range(range, opt, &handle_nc_bt, tx_opt)
            .instrument(trace_span!("btree file search range"))
            .await;
        self.debug_check_result(r).await
    }

    pub async fn insert<K, V, H, const TX_PROGRESS: TxProgress>(
        &self,
        key: &K,
        value: &V,
        opt: InsertOption,
        handle_write: &H,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        let handle_upsert_nc_bt = HandleWriteNCVBTree::<_, _, { TX_PROGRESS }>::new(
            &self.heap_file_committed,
            &self.heap_file_old_version,
            handle_write,
        );

        let r = self
            .btree_file
            .insert(key, value, opt, &handle_upsert_nc_bt, tx_opt)
            .instrument(trace_span!("btree file insert"))
            .await;
        self.debug_check_result(r).await
    }

    /// update a key/value pair
    pub async fn update<K, V, H, const TX_PROGRESS: TxProgress>(
        &self,
        key: &K,
        value: &V,
        opt: UpdateOption,
        handle_write: &H,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        let handle_upsert_nc_bt = HandleWriteNCVBTree::<_, _, { TX_PROGRESS }>::new(
            &self.heap_file_committed,
            &self.heap_file_old_version,
            handle_write,
        );

        let r = self
            .btree_file
            .update(key, value, opt, &handle_upsert_nc_bt, tx_opt)
            .instrument(trace_span!("btree file update"))
            .await;

        self.debug_check_result(r).await
    }

    /// delete a key/value pair
    pub async fn delete<K, H, const TX_PROGRESS: TxProgress>(
        &self,
        key: &K,
        opt: DeleteOption,
        handle_write: &H,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        H: HandleWrite,
    {
        let handle_upsert_nc_bt = HandleWriteNCVBTree::<_, _, { TX_PROGRESS }>::new(
            &self.heap_file_committed,
            &self.heap_file_old_version,
            handle_write,
        );

        let r = self
            .btree_file
            .delete(key, opt, &handle_upsert_nc_bt, tx_opt)
            .instrument(trace_span!("btree file delete"))
            .await;
        self.debug_check_result(r).await
    }

    pub async fn to_json(&self) -> Result<JsonValue> {
        Ok(object! {
            "btree" : self.btree_file.to_json().await?,
            "heap"  : self.heap_file_committed.to_json().await?,
        })
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
        self.btree_file
            .remove_lock(key, read, write, opt_cursor)
            .await
    }

    async fn debug_check_result<T>(&self, r: Result<T>) -> Result<T> {
        match &r {
            Err(e) => {
                if *e == ET::DebugNonLeafUpsertNone {
                    error!("{}", self.btree_file.to_json().await.unwrap().to_string());
                    error!("leaf upsert key non exist")
                }
            }
            _ => {}
        }
        r
    }
}

struct Pointer2Json {}

impl Default for Pointer2Json {
    fn default() -> Self {
        Self {}
    }
}

impl Clone for Pointer2Json {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl ToJson for Pointer2Json {
    fn to_json(&self, value: &[u8]) -> Result<JsonValue> {
        let pointer = SlotPointerRef::from(value);
        Ok(object! {
            "slot_no" : pointer.slot_no(),
            "page_id" : pointer.page_id(),
        })
    }
}

struct HandleReadNCVHeap<'a, F: HandleReadKV> {
    handler: &'a F,
    key: Arc<Mutex<Vec<u8>>>,
}

struct HandleReadNCVBTree<
    'b,
    'c,
    'd,
    F: HandleReadKV,
    T: ToJson,
    const TX_PROGRESS: TxProgress = { TxProgress::TxCommitted },
> {
    heap_file_committed: &'b HeapFile<T>,
    heap_file_old_version: &'c HeapFile<T>,
    handle_read_heap: HandleReadNCVHeap<'d, F>,
}

struct HandleWriteNCVBTree<
    'b,
    'c,
    'd,
    H: HandleWrite,
    T: ToJson,
    const TX_PROGRESS: TxProgress = { TxProgress::TxCommitted },
> {
    heap_file_committed: &'b HeapFile<T>,
    heap_file_old_version: &'c HeapFile<T>,
    handle_write: &'d H,
}

impl<'a, F: HandleReadKV> HandleReadNCVHeap<'a, F> {
    pub fn new(handler: &'a F) -> Self {
        Self {
            handler,
            key: Arc::new(Default::default()),
        }
    }

    pub async fn set_key(&self, key: &[u8]) {
        // copy key for later retrieve by heap callback handler
        let mut guard = self.key.lock().await;
        let vec = Vec::from(key);
        *guard = vec;
    }
}

unsafe impl<'a, H: HandleReadKV> Send for HandleReadNCVHeap<'a, H> {}

unsafe impl<'a, H: HandleReadKV> Sync for HandleReadNCVHeap<'a, H> {}

impl<'a, H: HandleReadKV> Clone for HandleReadNCVHeap<'a, H> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler,
            key: self.key.clone(),
        }
    }
}

#[async_trait]
impl<'a, F: HandleReadKV> HandleReadTuple for HandleReadNCVHeap<'a, F> {
    async fn on_read(&self, _oid: OID, tuple: &[u8]) -> Result<()> {
        let key = self.key.lock().await;
        self.handler.on_read(&key, tuple).await
    }
}

impl<'b, 'c, 'd, F: HandleReadKV, T: ToJson, const TX_PROGRESS: TxProgress>
    HandleReadNCVBTree<'b, 'c, 'd, F, T, TX_PROGRESS>
{
    pub fn new(
        heap_file_committed: &'b HeapFile<T>,
        heap_file_old_version: &'c HeapFile<T>,
        handle: HandleReadNCVHeap<'d, F>,
    ) -> Self {
        Self {
            heap_file_committed,
            heap_file_old_version,
            handle_read_heap: handle,
        }
    }
}

unsafe impl<'b, 'c, 'd, F: HandleReadKV, T: ToJson, const TX_PROGRESS: TxProgress> Send
    for HandleReadNCVBTree<'b, 'c, 'd, F, T, TX_PROGRESS>
{
}

unsafe impl<'b, 'c, 'd, F: HandleReadKV, T: ToJson, const TX_PROGRESS: TxProgress> Sync
    for HandleReadNCVBTree<'b, 'c, 'd, F, T, TX_PROGRESS>
{
}

impl<'b, 'c, 'd, F: HandleReadKV, T: ToJson, const TX_PROGRESS: TxProgress> Clone
    for HandleReadNCVBTree<'b, 'c, 'd, F, T, TX_PROGRESS>
{
    fn clone(&self) -> Self {
        Self {
            heap_file_committed: self.heap_file_committed,
            heap_file_old_version: self.heap_file_old_version,
            handle_read_heap: self.handle_read_heap.clone(),
        }
    }
}

#[async_trait]
impl<'b, 'c, 'd, F: HandleReadKV, T: ToJson, const TX_PROGRESS: TxProgress> HandleReadKV
    for HandleReadNCVBTree<'b, 'c, 'd, F, T, TX_PROGRESS>
{
    async fn on_read(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let (slot_no, page_id) = {
            let p = pointer_to_slot(value);
            (p.slot_no(), p.page_id())
        };
        if page_id != INVALID_PAGE_ID {
            let heap_file = self.heap_file_committed;
            self.handle_read_heap.set_key(key).await;
            heap_file
                .get(page_id, slot_no, &self.handle_read_heap)
                .await?;
        }
        Ok(())
    }
}

impl<'b, 'c, 'd, H: HandleWrite, T: ToJson, const TX_PROGRESS: TxProgress>
    HandleWriteNCVBTree<'b, 'c, 'd, H, T, TX_PROGRESS>
{
    pub fn new(
        heap_file_committed: &'b HeapFile<T>,
        heap_file_old_version: &'c HeapFile<T>,
        handle_upsert: &'d H,
    ) -> Self {
        HandleWriteNCVBTree {
            heap_file_committed,
            heap_file_old_version,
            handle_write: handle_upsert,
        }
    }
}

unsafe impl<'b, 'c, 'd, H: HandleWrite, T: ToJson, const TX_PROGRESS: TxProgress> Send
    for HandleWriteNCVBTree<'b, 'c, 'd, H, T, TX_PROGRESS>
{
}

unsafe impl<'b, 'c, 'd, H: HandleWrite, T: ToJson, const TX_PROGRESS: TxProgress> Sync
    for HandleWriteNCVBTree<'b, 'c, 'd, H, T, TX_PROGRESS>
{
}

impl<'b, 'c, 'd, H: HandleWrite, T: ToJson, const TX_PROGRESS: TxProgress> Clone
    for HandleWriteNCVBTree<'b, 'c, 'd, H, T, TX_PROGRESS>
{
    fn clone(&self) -> Self {
        Self {
            heap_file_committed: self.heap_file_committed,
            heap_file_old_version: self.heap_file_old_version,
            handle_write: self.handle_write,
        }
    }
}

#[async_trait]
impl<'b, 'c, 'd, H: HandleWrite, T: ToJson, const TX_PROGRESS: TxProgress> HandleWrite
    for HandleWriteNCVBTree<'b, 'c, 'd, H, T, TX_PROGRESS>
{
    async fn on_insert(&self, insert_value: &[u8]) -> Result<Option<Vec<u8>>> {
        let oid = gen_oid();
        let vec = if TX_PROGRESS == TxProgress::TxInProgress {
            SlotPointer::new(INVALID_PAGE_ID, 0).to_vec()
        } else {
            let heap_file = self.heap_file_committed;
            let pointer = heap_file.put(oid, insert_value, self.handle_write).await?;
            pointer.to_vec()
        };
        Ok(Some(vec))
    }

    async fn on_update(&self, update_value: &[u8], ref_value: &[u8]) -> Result<Option<Vec<u8>>> {
        if ref_value.len() < SlotPointerRef::len() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }
        if TX_PROGRESS == TxProgress::TxInProgress {
            return Ok(None); // do not change
        }

        let heap_file = self.heap_file_committed;
        let pointer = SlotPointerRef::from(ref_value);
        if pointer.is_invalid() {
            let oid = gen_oid();
            let heap_file = self.heap_file_committed;
            let pointer = heap_file.put(oid, update_value, self.handle_write).await?;
            Ok(Some(pointer.to_vec()))
        } else {
            let opt = heap_file
                .update(
                    pointer.page_id(),
                    pointer.slot_no(),
                    update_value,
                    self.handle_write,
                )
                .await?;
            Ok(opt.map(|p| p.to_vec()))
        }
    }

    async fn on_delete(&self, ref_value: &[u8]) -> Result<()> {
        if ref_value.len() < SlotPointerRef::len() {
            return Err(ET::ExceedCapacity);
        }
        if TX_PROGRESS == TxProgress::TxInProgress {
            return Ok(()); // do nothing
        }

        let heap_file = self.heap_file_committed;
        let pointer = SlotPointerRef::from(ref_value);

        heap_file
            .delete(pointer.page_id(), pointer.slot_no(), self.handle_write)
            .await?;
        Ok(())
    }
}
