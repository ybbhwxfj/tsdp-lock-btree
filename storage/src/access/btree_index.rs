/// non-cluster btree index
/// non-leaf page stores the pointer(page_id, slot_no) to a tuple in a heap file
///
use std::sync::Arc;

use async_trait::async_trait;
use json::{object, JsonValue};
use tokio::sync::Mutex;

use adt::compare::Compare;
use adt::range::RangeBounds;
use adt::slice::Slice;
use common::error_type::ET;
use common::id::{gen_oid, OID};
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, TxOption, UpdateOption};
use crate::access::btree_file::{BTreeFile, IsCluster};
use crate::access::buf_pool::BufPool;
use crate::access::file_id::FileId;
use crate::access::handle_read::{HandleReadKV, HandleReadTuple};
use crate::access::handle_write::HandleWrite;
use crate::access::heap_file::HeapFile;
use crate::access::slot_pointer::SlotPointerRef;
use crate::access::to_json::ToJson;

pub struct BTreeIndex<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson> {
    btree_file: BTreeFile<C, KT, Pointer2Json, { IsCluster::NonCluster }>,
    heap_file: HeapFile<VT>,
}

impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson> BTreeIndex<C, KT, VT> {
    pub fn new(
        key_cmp: C,
        key2json: KT,
        value2json: VT,
        btree_file: FileId,
        heap_file: FileId,
        pool: Arc<BufPool>,
    ) -> Self {
        let btree_file = BTreeFile::new(
            key_cmp,
            key2json,
            Pointer2Json::default(),
            btree_file,
            pool.clone(),
        );
        let heap_file = HeapFile::new(heap_file, value2json, pool.clone());

        BTreeIndex {
            btree_file,
            heap_file,
        }
    }
    pub fn enable_debug_latch(self, enable_debug_latch: bool) -> Self {
        let mut this = self;
        this.btree_file = this.btree_file.enable_debug_latch(enable_debug_latch);
        this
    }

    pub async fn open(&self) -> Result<()> {
        self.btree_file.open().await?;
        self.heap_file.open().await?;
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
        let handle_nc_heap = HandleReadNCHeap::new(handler);
        let handle_nc_bt = HandleReadNCBTree::new(&self.heap_file, handle_nc_heap);
        self.btree_file
            .search_key(key, opt, &handle_nc_bt, tx_opt)
            .await
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
        let handle_nc_heap = HandleReadNCHeap::new(handle_read);
        let handle_nc_bt = HandleReadNCBTree::new(&self.heap_file, handle_nc_heap);
        let n = self
            .btree_file
            .search_range(range, opt, &handle_nc_bt, tx_opt)
            .await?;
        Ok(n)
    }

    pub async fn insert<K, V, H>(
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
        let handle_upsert_nc_bt = HandleWriteNCBTree::new(&self.heap_file, handle_write);

        self.btree_file
            .insert(key, value, opt, &handle_upsert_nc_bt, tx_opt)
            .await
    }

    /// update a key/value pair
    pub async fn update<K, V, H>(
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
        let handle_upsert_nc_bt = HandleWriteNCBTree::new(&self.heap_file, handle_write);

        let ok = self
            .btree_file
            .update(key, value, opt, &handle_upsert_nc_bt, tx_opt)
            .await?;
        Ok(ok)
    }

    /// delete a key/value pair
    pub async fn delete<K, H>(
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
        self.btree_file.delete(key, opt, handle_write, tx_opt).await
    }

    pub async fn to_json(&self) -> Result<JsonValue> {
        Ok(object! {
            "btree" : self.btree_file.to_json().await?,
            "heap"  : self.heap_file.to_json().await?,
        })
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

struct HandleReadNCHeap<'a, F: HandleReadKV> {
    handler: &'a F,
    key: Arc<Mutex<Vec<u8>>>,
}

struct HandleReadNCBTree<'a, 'b, F: HandleReadKV, T: ToJson> {
    heap_file: &'a HeapFile<T>,
    handle_read_heap: HandleReadNCHeap<'b, F>,
}

struct HandleWriteNCBTree<'a, 'b, H: HandleWrite, T: ToJson> {
    handle_write: &'b H,
    heap_file: &'a HeapFile<T>,
}

impl<'a, F: HandleReadKV> HandleReadNCHeap<'a, F> {
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

unsafe impl<'a, H: HandleReadKV> Send for HandleReadNCHeap<'a, H> {}

unsafe impl<'a, H: HandleReadKV> Sync for HandleReadNCHeap<'a, H> {}

impl<'a, H: HandleReadKV> Clone for HandleReadNCHeap<'a, H> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler,
            key: self.key.clone(),
        }
    }
}

#[async_trait]
impl<'a, F: HandleReadKV> HandleReadTuple for HandleReadNCHeap<'a, F> {
    async fn on_read(&self, _oid: OID, tuple: &[u8]) -> Result<()> {
        let key = self.key.lock().await;
        self.handler.on_read(&key, tuple).await
    }
}

impl<'a, 'b, F: HandleReadKV, T: ToJson> HandleReadNCBTree<'a, 'b, F, T> {
    pub fn new(heap_file: &'a HeapFile<T>, handle: HandleReadNCHeap<'b, F>) -> Self {
        HandleReadNCBTree {
            handle_read_heap: handle,
            heap_file,
        }
    }
}

unsafe impl<'a, 'b, F: HandleReadKV, T: ToJson> Send for HandleReadNCBTree<'a, 'b, F, T> {}

unsafe impl<'a, 'b, AF: HandleReadKV, T: ToJson> Sync for HandleReadNCBTree<'a, 'b, AF, T> {}

impl<'a, 'b, AF: HandleReadKV, T: ToJson> Clone for HandleReadNCBTree<'a, 'b, AF, T> {
    fn clone(&self) -> Self {
        Self {
            handle_read_heap: self.handle_read_heap.clone(),
            heap_file: self.heap_file,
        }
    }
}

#[async_trait]
impl<'a, 'b, AF: HandleReadKV, T: ToJson> HandleReadKV for HandleReadNCBTree<'a, 'b, AF, T> {
    async fn on_read(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let pointer = SlotPointerRef::from(value);
        let slot_no = pointer.slot_no();
        let page_id = pointer.page_id();
        self.handle_read_heap.set_key(key).await;
        self.heap_file
            .get(page_id, slot_no, &self.handle_read_heap)
            .await?;
        Ok(())
    }
}

impl<'a, 'b, H: HandleWrite, T: ToJson> HandleWriteNCBTree<'a, 'b, H, T> {
    pub fn new(heap_file: &'a HeapFile<T>, handle_upsert: &'b H) -> Self {
        HandleWriteNCBTree {
            handle_write: handle_upsert,
            heap_file,
        }
    }
}

unsafe impl<'a, 'b, H: HandleWrite, T: ToJson> Send for HandleWriteNCBTree<'a, 'b, H, T> {}

unsafe impl<'a, 'b, H: HandleWrite, T: ToJson> Sync for HandleWriteNCBTree<'a, 'b, H, T> {}

impl<'a, 'b, H: HandleWrite, T: ToJson> Clone for HandleWriteNCBTree<'a, 'b, H, T> {
    fn clone(&self) -> Self {
        Self {
            handle_write: self.handle_write,
            heap_file: self.heap_file,
        }
    }
}

#[async_trait]
impl<'a, 'b, H: HandleWrite, T: ToJson> HandleWrite for HandleWriteNCBTree<'a, 'b, H, T> {
    async fn on_insert(&self, insert_value: &[u8]) -> Result<Option<Vec<u8>>> {
        let oid = gen_oid();
        let pointer = self
            .heap_file
            .put(oid, insert_value, self.handle_write)
            .await?;
        Ok(Some(pointer.to_vec()))
    }

    async fn on_update(&self, update_value: &[u8], ref_value: &[u8]) -> Result<Option<Vec<u8>>> {
        if ref_value.len() < SlotPointerRef::len() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        let pointer = SlotPointerRef::from(ref_value);
        let opt = self
            .heap_file
            .update(
                pointer.page_id(),
                pointer.slot_no(),
                update_value,
                self.handle_write,
            )
            .await?;
        Ok(opt.map(|p| p.to_vec()))
    }

    async fn on_delete(&self, ref_value: &[u8]) -> Result<()> {
        if ref_value.len() < SlotPointerRef::len() {
            assert!(false);
            return Err(ET::ExceedCapacity);
        }

        let pointer = SlotPointerRef::from(ref_value);

        self.heap_file
            .delete(pointer.page_id(), pointer.slot_no(), self.handle_write)
            .await?;
        Ok(())
    }
}
