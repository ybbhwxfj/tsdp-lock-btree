/// buffer pool
use std::collections::{HashSet, VecDeque};

use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use async_std::sync::{Condvar, Mutex};

use scc::ebr::Barrier;
use scc::HashIndex;
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncWriteExt};
use tokio::sync::Notify;
use tracing::{info_span, trace_span, Instrument};

use adt::compare::Compare;
use common::error_type::ET;
use common::id::gen_oid;
use common::result::{io_result, Result};

use crate::access::bt_pred::{BtPred, BtPredCtx, BtPredTable};
use crate::access::const_val::{file_type_to_ext, FileKind};
use crate::access::file::{
    async_open_file, async_seek, async_write_all, file_id_to_name, path_file_open,
};
use crate::access::file_id::FileId;

use crate::access::latch_type::PLatchType;
use crate::access::page_hdr::PageHdr;
use crate::access::page_id::{page_id_to_offset, PageId, PageIndex};
use crate::access::page_latch::{PLatchEP, PageGuardEP, PageLatchEP};
use crate::access::predicate::{
    EmptyPred, EmptyPredCtx, EmptyPredTable, Pred, PredCtx, PredTable, PredTableBase,
};
use crate::access::space_mgr::SpaceMgr;

enum FlushOpType {
    DirtyPage(PageIndex),
    Checkpoint(Notify),
    StopPool,
}

struct FlushQueue {
    queue: VecDeque<FlushOpType>,
}

//	The memory mapping pool table buffer manager entry
#[derive(Clone)]
pub struct BufPool {
    base_path: String,
    page_size: u64,
    page_num: u64,
    flush_dirty_num: u64,
    pool_map: Arc<HashIndex<PageIndex, PLatchEP<Vec<u8>>>>,
    sender: Arc<Sender<FlushOpType>>,
    receiver: Arc<Receiver<FlushOpType>>,
    // total page count, lock as declare order
    total_page_num: Arc<Mutex<u64>>,

    flush_queue: Arc<Mutex<FlushQueue>>,
    flush_cond: Arc<Condvar>,
}

impl Default for FlushQueue {
    fn default() -> Self {
        Self {
            queue: Default::default(),
        }
    }
}

impl BufPool {
    pub fn new(buff_size: u64, page_size: u64, base_path: String) -> BufPool {
        let page_size_ = page_size as usize;
        let buff_size_ = buff_size as usize;
        let page_num = buff_size_ / page_size_;
        let (sender, receiver) = async_channel::unbounded();
        let bp = BufPool {
            base_path,
            page_size,
            page_num: page_num as u64,
            flush_dirty_num: 2000,
            pool_map: Default::default(),
            sender: Arc::new(sender),
            receiver: Arc::new(receiver),
            total_page_num: Default::default(),
            flush_queue: Default::default(),
            flush_cond: Default::default(),
        };
        bp
    }

    pub fn path(&self) -> String {
        self.base_path.clone()
    }

    pub async fn access_path_open<PT: PredTableBase + 'static>(
        &self,
        file_id: FileId,
        kind: FileKind,
        space_mgr: &SpaceMgr,
        page_init: fn(&mut Vec<u8>) -> Result<()>,
        page_is_empty: fn(&mut Vec<u8>) -> Result<bool>,
    ) -> Result<()> {
        let file_name = format!("{}.{}", file_id_to_name(file_id), file_type_to_ext(kind));
        let path_buf = Path::new(&self.base_path).join(file_name);
        let path = path_buf.as_path();
        if !Path::new(path).exists() {
            let mut fd = async_open_file(path).await?;
            let r = fd.flush().await;
            io_result(r)?;

            let mut vec: Vec<u8> = Vec::new();
            vec.resize(self.page_size as usize, 0);
            page_init(&mut vec)?;
            let w_r = fd.write(vec.as_slice()).await;
            match w_r {
                Ok(s) => {
                    if s != vec.len() {
                        return Err(ET::IOError("write error length".to_string()));
                    }
                }
                Err(e) => {
                    return Err(ET::IOError(e.to_string()));
                }
            }
            let f_r = fd.flush().await;
            match f_r {
                Ok(()) => {}
                Err(e) => {
                    return Err(ET::IOError(e.to_string()));
                }
            }
        }

        let mut fd = async_open_file(path).await?;
        let mut result = Ok(());
        let mut page_id = 0 as PageId;
        let mut empty_page = vec![];

        while result.is_ok() {
            let mut vec: Vec<u8> = Vec::new();
            vec.resize(self.page_size as usize, 0);
            match fd.read(vec.as_mut_slice()).await {
                Ok(n) => {
                    if n == 0 {
                        break;
                    } else if n != vec.len() {
                        result = Err(ET::IOError("read error".to_string()));
                    }
                }
                Err(e) => {
                    result = Err(ET::IOError(e.to_string()));
                }
            }

            if result.is_ok() {
                if page_is_empty(&mut vec)? {
                    // empty page
                    empty_page.push(page_id);
                } else {
                    self.add_page::<PT>(PageIndex::new(file_id, page_id), vec)
                        .await?
                }
            } else {
                return result;
            }

            page_id += 1;
        }
        for id in empty_page {
            if id != 0 && id != page_id {
                // only the first and last page is allowed to be empty
                space_mgr.page_removed(id).await;
            }
        }
        space_mgr.set_page_num(page_id).await;
        return Ok(());
    }

    pub async fn get_all_page(&self) -> Vec<PageGuardEP> {
        self.get_all_page_gut().await
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub async fn add_dirty(&self, page_index: PageIndex) -> Result<()> {
        self.add_dirty_gut(page_index).await
    }

    pub async fn loop_flush(&self) -> Result<()> {
        loop {
            let to_flush = {
                let mut deque = self.flush_queue.lock().await;
                while deque.queue.len() == 0 {
                    deque = self.flush_cond.wait(deque).await;
                }
                let mut clone: VecDeque<FlushOpType> = VecDeque::new();
                std::mem::swap(&mut deque.queue, &mut clone);
                clone
            };
            for op in to_flush {
                match op {
                    FlushOpType::DirtyPage(id) => {
                        self.flush_page(&id).await?;
                    }
                    FlushOpType::Checkpoint(notify) => {
                        notify.notify_one();
                    }
                    FlushOpType::StopPool => {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn collecting_dirty_page_id(&self) -> Result<()> {
        let mut dirty = HashSet::new();
        while self.continue_flush(&mut dirty).await? {}
        Ok(())
    }

    /// when the system restarted, the database load pages to buffer pool
    /// this function would call
    /// general processing when system running, call get_page to load
    pub async fn add_page<PT: PredTableBase + 'static>(
        &self,
        page_index: PageIndex,
        vec: Vec<u8>,
    ) -> Result<()> {
        if vec.len() != self.page_size as usize {
            panic!("error size");
        }
        // lock by declare order
        let mut num = self.total_page_num.lock().await;

        match self
            .insert(
                page_index,
                PLatchEP::new(page_index, vec, Box::new(PT::default())),
            )
            .await
        {
            Some(_) => {
                *num = *num + 1;
                panic!("error");
            }
            None => Ok(()),
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn read_page_from_file<const T: FileKind>(
        &self,
        page_index: &PageIndex,
        buf: &mut [u8],
    ) -> Result<()> {
        assert_eq!(buf.len(), self.page_size as usize);
        let ext_str = file_type_to_ext(T);
        let mut fd = path_file_open(&self.base_path, page_index.file_id(), ext_str).await?;

        let offset = page_id_to_offset(page_index.page_id(), self.page_size);
        let seek_r = Pin::new(&mut fd).start_seek(SeekFrom::Start(offset));
        io_result(seek_r)?;

        let read_r = fd.read(buf).instrument(trace_span!("File::read")).await;
        match read_r {
            Ok(size) => {
                if size != buf.len() {
                    if size == 0 {
                        return Err(ET::IOError("read EOF".to_string()));
                    }
                    panic!("error size");
                }
            }
            Err(e) => {
                return Err(ET::IOError(e.to_string()));
            }
        }
        Ok(())
    }

    pub async fn get_bt_page<C: Compare<[u8]> + 'static>(
        &self,
        index: &PageIndex,
    ) -> Result<PLatchEP<Vec<u8>>> {
        self.get_page::<{ FileKind::FileBTree }, BtPredCtx<C>, BtPred, BtPredTable>(&index)
            .await
    }

    pub async fn get_hp_page(&self, index: &PageIndex) -> Result<PLatchEP<Vec<u8>>> {
        self.get_page::<{ FileKind::FileHeap }, EmptyPredCtx, EmptyPred, EmptyPredTable>(&index)
            .await
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub async fn get_page<
        const T: FileKind,
        C: PredCtx,
        P: Pred<C>,
        PT: PredTable<C, P> + 'static,
    >(
        &self,
        index: &PageIndex,
    ) -> Result<PLatchEP<Vec<u8>>> {
        let opt = self.get(index).await;
        return match opt {
            Some(page) => Ok(page.clone()),
            None => {
                // not in the buffer pool
                let mut vec = Vec::new();
                vec.resize(self.page_size as usize, 0);
                let _ = self
                    .read_page_from_file::<T>(index, vec.as_mut_slice())
                    .instrument(trace_span!("read_page_from_file"))
                    .await?;
                let new_page = PLatchEP::new(*index, vec, Box::new(PT::default()));
                let _ = self.insert(*index, new_page.clone()).await;
                Ok(new_page)
            }
        };
    }

    pub async fn allocate_empty_page<F, C, P, S>(
        &self,
        page_index: &PageIndex,
        init_page: F,
    ) -> Result<PageLatchEP>
    where
        F: Fn(&mut Vec<u8>) -> Result<()>,
        C: PredCtx,
        P: Pred<C>,
        S: PredTable<C, P> + 'static,
    {
        let mut n = self.total_page_num.lock().await;
        if *n <= self.page_num {
            let mut vec = Vec::new();
            vec.resize(self.page_size as usize, 0);
            init_page(&mut vec)?;
            let page = PageLatchEP::new(*page_index, vec, Box::new(S::default()));
            let ret = self.insert(page_index.clone(), page.clone()).await;
            if ret.is_some() {
                return Err(ET::ExistingSuchElement);
            } else {
                *n = *n + 1;
                Ok(page)
            }
        } else {
            panic!("memory overflow, TODO buffer replace policy");
            // Err(ET::NoneOption)
        }
    }

    pub async fn checkpoint(&self) -> Result<()> {
        Ok(())
    }

    pub async fn close(&self) {
        {
            let _ = self.sender.send(FlushOpType::StopPool).await;
        }
        {
            let mut queue = self.flush_queue.lock().await;
            queue.queue.push_back(FlushOpType::StopPool);
            self.flush_cond.notify_one();
        }
    }

    async fn get(&self, index: &PageIndex) -> Option<PageLatchEP> {
        let barrier = Barrier::new();
        let opt = self
            .pool_map
            .read_with(&index, |_k, _v| _v.clone(), &barrier);
        opt
    }

    async fn insert(&self, index: PageIndex, latch: PageLatchEP) -> Option<PageLatchEP> {
        let result = self.pool_map.insert_async(index, latch).await;
        match result {
            Ok(_) => None,
            Err((_, _v)) => {
                panic!("error");
            }
        }
    }

    async fn add_dirty_gut(&self, index: PageIndex) -> Result<()> {
        let r = self.sender.send(FlushOpType::DirtyPage(index)).await;
        match r {
            Ok(_) => Ok(()),
            Err(_e) => {
                return Err(ET::ChRecvError);
            }
        }
    }

    async fn add_to_flush_dirty_page(
        &self,
        dirty: &mut HashSet<PageIndex>,
        opt_notify: Option<Notify>,
    ) {
        let mut deque = self.flush_queue.lock().await;
        for id in dirty.iter() {
            deque.queue.push_back(FlushOpType::DirtyPage(*id));
        }
        match opt_notify {
            Some(n) => deque.queue.push_back(FlushOpType::Checkpoint(n)),
            _ => {}
        }
    }

    async fn continue_flush(&self, dirty: &mut HashSet<PageIndex>) -> Result<bool> {
        let r = self.receiver.recv().await;
        let flush_op = match r {
            Ok(id) => id,
            Err(_e) => {
                return Err(ET::ChRecvError);
            }
        };
        match flush_op {
            FlushOpType::DirtyPage(id) => {
                let _ = dirty.insert(id);
                if dirty.len() > self.flush_dirty_num as usize {
                    self.add_to_flush_dirty_page(dirty, None).await;
                    self.flush_cond.notify_one();
                }
                Ok(true)
            }
            FlushOpType::Checkpoint(notify) => {
                self.add_to_flush_dirty_page(dirty, Some(notify)).await;
                self.flush_cond.notify_one();
                Ok(true)
            }
            FlushOpType::StopPool => Ok(false),
        }
    }

    async fn flush_page(&self, index: &PageIndex) -> Result<()> {
        let data = {
            let opt = {
                let barrier = Barrier::new();
                self.pool_map.read_with(index, |_, v| v.clone(), &barrier)
            };
            let latch = match opt {
                Some(g) => g,
                None => {
                    // no such page
                    return Ok(());
                }
            };
            let guard = latch.lock(PLatchType::ReadLock, gen_oid()).await;
            let vec = (*guard).clone();
            guard.unlock().instrument(info_span!("Guard::unlock")).await;
            vec
        };

        let header = PageHdr::from(&data);
        let file_type = header.get_type();
        // write to file, keeps no page locks...
        let ext_str = file_type_to_ext(file_type);
        let mut file = path_file_open(&self.base_path, index.file_id(), ext_str).await?;
        let offset = page_id_to_offset(index.page_id(), self.page_size);
        async_seek(&mut file, offset).await?;
        async_write_all(&mut file, data.as_slice()).await?;
        Ok(())
    }

    async fn get_all_page_gut(&self) -> Vec<PageGuardEP> {
        let mut vec = Vec::new();
        let latch = {
            let barrier = Barrier::new();
            let mut vec_latch = Vec::new();
            for (_, latch) in self.pool_map.iter(&barrier) {
                vec_latch.push(latch.clone())
            }
            vec_latch
        };

        for l in latch {
            let guard = l.lock(PLatchType::None, gen_oid()).await;
            vec.push(guard);
        }
        vec
    }
}
