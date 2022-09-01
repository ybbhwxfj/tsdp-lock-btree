/// heap file
/// An unordered collection of pages where tuples are stored in random order.
///
use std::collections::HashMap;
use std::sync::Arc;

use json::{array, object, JsonValue};
use log::debug;

use common::error_type::ET;
use common::id::{gen_oid, TaskId, OID};
use common::result::Result;

use crate::access::buf_pool::BufPool;
use crate::access::const_val::{FileKind, INVALID_PAGE_ID};
use crate::access::file_id::FileId;
use crate::access::handle_read::HandleReadTuple;
use crate::access::handle_write::HandleWrite;
use crate::access::latch_type::PLatchType;
use crate::access::page_hp::{PageHeap, PageHeapMut};
use crate::access::page_id::{PageId, PageIndex};
use crate::access::page_latch::{
    guard_add, guard_release_all, guard_release_one, PageGuardEP, PageLatchEP,
};
use crate::access::predicate::{EmptyPred, EmptyPredCtx, EmptyPredTable};
use crate::access::slot_pointer::SlotPointer;
use crate::access::space_mgr::SpaceMgr;
use crate::access::to_json::ToJson;

#[derive(Clone)]
pub struct HeapFile<T: ToJson> {
    file_id: FileId,
    to_json: T,
    space_mgr: SpaceMgr,
    pool: Arc<BufPool>,
}

unsafe impl<T: ToJson> Send for HeapFile<T> {}

unsafe impl<T: ToJson> Sync for HeapFile<T> {}

impl<T: ToJson> HeapFile<T> {
    pub fn new(file_id: FileId, to_json: T, pool: Arc<BufPool>) -> Self {
        HeapFile {
            file_id,
            to_json,
            space_mgr: SpaceMgr::new(),
            pool,
        }
    }

    pub async fn open(&self) -> Result<()> {
        let init = |vec: &mut Vec<u8>| {
            let mut page = PageHeapMut::from(vec);
            page.format(0, INVALID_PAGE_ID, INVALID_PAGE_ID);
            Ok(())
        };
        let is_empty = |vec: &mut Vec<u8>| {
            let page = PageHeap::from(vec);
            Ok(page.get_active_count() == 0)
        };
        self.pool
            .access_path_open::<EmptyPredTable>(
                self.file_id,
                FileKind::FileHeap,
                &self.space_mgr,
                init,
                is_empty,
            )
            .await
    }

    pub async fn scan(&self, _fn_scan: fn(OID, &[u8]) -> Result<()>) -> Result<()> {
        Ok(())
    }

    pub async fn put<F>(&self, oid: OID, tuple: &[u8], handle_upsert: &F) -> Result<SlotPointer>
    where
        F: HandleWrite,
    {
        let task_id = gen_oid();
        debug!("heap file task id {}", task_id);
        let mut guards = HashMap::new();
        let r = self
            .put_gut(oid, tuple, &self.pool, handle_upsert, &mut guards, task_id)
            .await;
        guard_release_all(&mut guards).await;
        return r;
    }

    pub async fn get<F>(&self, page_id: PageId, slot_no: u32, handle_upsert: &F) -> Result<()>
    where
        F: HandleReadTuple,
    {
        let task_id = gen_oid();
        debug!("heap file task id {}", task_id);
        let mut guards = HashMap::new();
        let r = self
            .get_gut(page_id, slot_no, handle_upsert, &mut guards, task_id)
            .await;
        guard_release_all(&mut guards).await;
        return r;
    }

    /// return the pointer to a slot updated
    pub async fn update<H>(
        &self,
        page_id: PageId,
        index: u32,
        tuple: &[u8],
        handle_upsert: &H,
    ) -> Result<Option<SlotPointer>>
    where
        H: HandleWrite,
    {
        let task_id = gen_oid();
        debug!("heap file task id {}", task_id);
        let mut guards = HashMap::new();
        let r = self
            .update_gut(
                page_id,
                index,
                tuple,
                &self.pool,
                handle_upsert,
                &mut guards,
                task_id,
            )
            .await;
        guard_release_all(&mut guards).await;
        return r;
    }

    /// delete a slot
    pub async fn delete<H>(&self, page_id: PageId, index: u32, handle_upsert: &H) -> Result<()>
    where
        H: HandleWrite,
    {
        let task_id = gen_oid();
        debug!("heap file task id {}", task_id);
        let mut guards = HashMap::new();
        let r = self
            .delete_gut(
                page_id,
                index,
                &self.pool,
                handle_upsert,
                &mut guards,
                task_id,
            )
            .await;
        guard_release_all(&mut guards).await;
        return r;
    }

    async fn update_gut<H>(
        &self,
        page_id: PageId,
        index: u32,
        update_tuple: &[u8],
        pool: &BufPool,
        handle_upsert: &H,
        guards: &mut HashMap<PageId, PageGuardEP>,
        task_id: TaskId,
    ) -> Result<Option<SlotPointer>>
    where
        H: HandleWrite,
    {
        let latch = pool
            .get_hp_page(&PageIndex::new(self.file_id, page_id))
            .await?;
        let mut guard = latch.lock(PLatchType::WriteLock, task_id).await;
        guard_add(guards, guard.page_id(), guard.clone());

        let will_update_ok = {
            let hp_page = PageHeap::from(&mut guard);
            let (_, old_tuple) = hp_page.get_tuple(index);
            handle_upsert.on_update(update_tuple, old_tuple).await?;
            let ok = hp_page.will_update_ok(index, update_tuple)?;
            ok
        };

        if will_update_ok {
            let mut hp_page = PageHeapMut::from(&mut guard);
            hp_page.update(index, None, update_tuple);
        };

        return if !will_update_ok {
            let tuple_id = {
                let mut hp_page = PageHeapMut::from(&mut guard);
                let tuple_id = hp_page.remove(index)?;
                tuple_id
            };
            guard_release_one(guards, guard.page_id()).await;
            drop(guard);
            let pointer = self
                .put_gut(tuple_id, update_tuple, pool, handle_upsert, guards, task_id)
                .await?;
            Ok(Some(pointer))
        } else {
            Ok(None)
        };
    }

    pub async fn get_gut<F>(
        &self,
        page_id: PageId,
        slot_no: u32,
        handler: &F,
        guards: &mut HashMap<PageId, PageGuardEP>,
        task_id: TaskId,
    ) -> Result<()>
    where
        F: HandleReadTuple,
    {
        let latch = self
            .pool
            .get_hp_page(&PageIndex::new(self.file_id, page_id))
            .await?;
        let guard = latch.lock(PLatchType::ReadLock, task_id).await;
        guard_add(guards, guard.page_id(), guard.clone());
        let (oid, tuple) = {
            let page = PageHeap::from(&guard);
            if slot_no >= page.get_count() {
                return Err(ET::NoSuchElement);
            }
            let (oid, tuple) = page.get_tuple(slot_no);
            (oid, Vec::from(tuple))
        };

        handler.on_read(oid, tuple.as_slice()).await?;

        return Ok(());
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn put_gut<F>(
        &self,
        oid: OID,
        tuple: &[u8],
        pool: &BufPool,
        handle_upsert: &F,
        guards: &mut HashMap<PageId, PageGuardEP>,
        task_id: TaskId,
    ) -> Result<SlotPointer>
    where
        F: HandleWrite,
    {
        let root_id = self.space_mgr.max_page_id().await;
        let latch = pool
            .get_hp_page(&PageIndex::new(self.file_id, root_id))
            .await?;
        let mut guard = latch.lock(PLatchType::WriteLock, task_id).await;
        guard_add(guards, guard.page_id(), guard.clone());

        let opt_index = {
            let mut page = PageHeapMut::from(&mut guard);
            page.put(oid, tuple)
        };

        let (page_id, slot_no) = match opt_index {
            Some(slot_no) => (guard.page_id(), slot_no),
            None => {
                guard_release_one(guards, guard.page_id()).await;
                drop(guard);
                let latch_n = self.allocate_page(pool).await?;
                let mut guard_n = latch_n.lock(PLatchType::WriteLock, task_id).await;
                guard_add(guards, guard_n.page_id(), guard_n.clone());
                let mut page_n = PageHeapMut::from(&mut guard_n);
                let opt = page_n.put(oid, tuple);
                match opt {
                    Some(slot_no) => (guard_n.page_id(), slot_no),
                    None => {
                        return Err(ET::ExceedCapacity);
                    }
                }
            }
        };

        handle_upsert.on_insert(tuple).await?;
        Ok(SlotPointer::new(page_id, slot_no))
    }

    async fn allocate_page(&self, pool: &BufPool) -> Result<PageLatchEP> {
        let page_id = self.space_mgr.new_page_id().await;
        let init_page = |vec: &mut Vec<u8>| {
            let mut page = PageHeapMut::from(vec);
            page.format(page_id, INVALID_PAGE_ID, INVALID_PAGE_ID);
            Ok(())
        };
        pool.allocate_empty_page::<_, EmptyPredCtx, EmptyPred, EmptyPredTable>(
            &PageIndex::new(self.file_id, page_id),
            init_page,
        )
        .await
    }

    async fn delete_gut<H>(
        &self,
        _page_id: PageId,
        _index: u32,
        _pool: &BufPool,
        _handle_upsert: &H,
        _guards: &mut HashMap<PageId, PageGuardEP>,
        _task_id: TaskId,
    ) -> Result<()> {
        todo!()
    }

    pub async fn to_json(&self) -> Result<JsonValue> {
        let mut page_id = 0;
        let mut array = array![];
        loop {
            let r = self
                .pool
                .get_hp_page(&PageIndex::new(self.file_id, page_id))
                .await;
            match r {
                Ok(l) => {
                    let json = {
                        let guard = l.lock(PLatchType::ReadLock, 0).await;
                        let page = PageHeap::from(&guard);
                        let r = page.to_json(&self.to_json);
                        guard.unlock().await;
                        r?
                    };
                    array.push(json).expect("push json error");
                }
                Err(_e) => {
                    break;
                }
            }
            page_id += 1;
        }

        Ok(object! {"pages" : array})
    }
}
