use std::collections::{BTreeMap, HashMap, HashSet};

use std::ops::Bound;
use std::time::SystemTime;

use tracing::{debug, error, info, info_span, trace};
use tracing_futures::Instrument;

use adt::compare::Compare;
use common::id::{gen_oid, TaskId, OID};
use common::result::Result;

use crate::access::bt_pred::{BtPred, BtPredCtx, BtPredTable};
use crate::access::buf_pool::BufPool;
use crate::access::const_val::INVALID_PAGE_ID;
use crate::access::file_id::FileId;
use crate::access::latch_type::PLatchType;
use crate::access::latch_type::PLatchType::ParentModification;
use crate::access::page_bt::{PageBTreeMut, PageHdrBt};
use crate::access::page_bt::{LEAF_PAGE, NON_LEAF_PAGE};
use crate::access::page_id::{PageId, PageIndex};
use crate::access::page_latch::{PageGuardEP, PageLatchEP};

struct DebugContext {
    task_id: TaskId,
    level2max: HashMap<i64, i64>,
    page2location: HashMap<(PageId, PLatchType), (i64, i64)>,
    location2page: BTreeMap<(i64, i64), (PageId, PLatchType)>,
    system_time: SystemTime,
}

pub struct GuardCollection {
    file_id: FileId,
    task_id: TaskId,
    guard: HashMap<(PageId, PLatchType), PageGuardEP>,
    current_level: HashSet<(PageId, PLatchType)>,
    opt_debug_level: Option<u32>,
    opt_debug_context: Option<DebugContext>,
}

impl DebugContext {
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            level2max: Default::default(),
            page2location: Default::default(),
            location2page: Default::default(),
            system_time: SystemTime::now(),
        }
    }

    pub fn test_elapse(&self) {
        match self.system_time.elapsed() {
            Ok(_elapsed) => {
                if _elapsed.as_secs() > 100 {
                    info!("elapsed exceed 100sec")
                }

                // assert!(elapsed.as_secs() < 100);
            }
            Err(_) => {}
        }
    }
    /// task is only for debug
    #[allow(dead_code)]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl GuardCollection {
    pub fn new(file_id: OID) -> Self {
        Self {
            file_id,
            task_id: gen_oid(),
            guard: Default::default(),
            current_level: Default::default(),
            opt_debug_level: None,
            opt_debug_context: None,
        }
    }

    pub fn enable_debug_latch(self, debug: bool) -> Self {
        let mut this = self;
        if debug {
            this.opt_debug_context = Some(DebugContext::new(this.task_id));
        } else {
            this.opt_debug_context = None;
        }
        this
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub async fn intent_access_and_upgrade<C: Compare<[u8]> + 'static>(
        &mut self,
        pool: &BufPool,
        page_id: PageId,
        l_type: PLatchType,

        opt_release: Option<PageGuardEP>,
    ) -> Result<PageGuardEP> {
        let guard1 = self.lock_page_intent::<C>(pool, page_id, l_type).await?;
        match opt_release {
            Some(g) => {
                self.unlock_page(g.page_id(), g.latch_type()).await;
            }
            None => {}
        };
        let guard2 = if guard1.latch_type() == PLatchType::AccessIntent {
            self.upgrade(page_id, PLatchType::AccessIntent, l_type)
                .await?
        } else {
            guard1
        };
        Ok(guard2)
    }

    pub async fn lock_page_intent<C: Compare<[u8]> + 'static>(
        &mut self,
        pool: &BufPool,
        page_id: PageId,
        l_type: PLatchType,
    ) -> Result<PageGuardEP> {
        if !(l_type == PLatchType::ReadLock || l_type == PLatchType::WriteLock) {
            panic!("only allow either ReadLock or WriteLock")
        }
        let opt = self.guard.get(&(page_id, l_type));
        match opt {
            Some(g) => {
                return Ok(g.clone());
            }
            None => {}
        }
        let opt = self.guard.get(&(page_id, PLatchType::AccessIntent));
        let intent_lock = match opt {
            Some(g) => g.clone(),
            None => {
                self.lock_bt_page_level::<C>(pool, page_id, PLatchType::AccessIntent, true)
                    .await?
            }
        };
        Ok(intent_lock)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub async fn lock_bt_page_level<C: Compare<[u8]> + 'static>(
        &mut self,
        pool: &BufPool,
        page_id: PageId,
        l_type: PLatchType,
        record_level: bool,
    ) -> Result<PageGuardEP> {
        debug!("latch page");
        let opt = self.guard.get(&(page_id, l_type));
        match opt {
            Some(t) => {
                let l = t.clone();
                return Ok(l);
            }
            None => {}
        }

        let latch: PageLatchEP = pool
            .get_bt_page::<C>(&PageIndex::new(self.file_id, page_id))
            .instrument(info_span!("BtPool::get_page"))
            .await?;

        let guard = latch
            .lock(l_type, self.task_id())
            .instrument(info_span!("lock page"))
            .await;

        let level = {
            let hdr = PageHdrBt::from(&guard);
            let level = hdr.get_page_level();
            guard.set_level(level).await;
            level
        };
        if record_level {
            self.add_current_level(page_id, l_type, Some(level))
        }

        let opt = self.guard.insert((page_id, l_type), guard.clone());
        assert!(opt.is_none());

        self.test_lock_order(page_id, level, l_type).await;
        Ok(guard)
    }

    pub async fn upgrade(
        &mut self,
        page_id: PageId,
        old_l_type: PLatchType,
        new_l_type: PLatchType,
    ) -> Result<PageGuardEP> {
        self.upgrade_gut(page_id, old_l_type, new_l_type).await
    }

    async fn upgrade_gut(
        &mut self,
        page_id: PageId,
        old_l_type: PLatchType,
        new_l_type: PLatchType,
    ) -> Result<PageGuardEP> {
        let _ = self.current_level.remove(&(page_id, old_l_type));

        let opt_guard = self.guard.remove(&(page_id, old_l_type));
        let new_guard = match opt_guard {
            Some(guard) => {
                let new_guard = guard.upgrade(new_l_type).await;
                trace!("upgrade page {:?}", (page_id, new_l_type));
                let _ = self.guard.insert((page_id, new_l_type), new_guard.clone());

                let level = {
                    let hdr = PageHdrBt::from(&guard);
                    let level = hdr.get_page_level();
                    guard.set_level(level).await;
                    level
                };
                self.add_current_level(page_id, new_l_type, Some(level));
                new_guard
            }
            None => {
                panic!("error")
            }
        };
        Ok(new_guard)
    }
    pub async fn unlock_page(&mut self, page_id: PageId, l_type: PLatchType) {
        debug!(
            "after latch unlock {:?}, task {:?}",
            page_id,
            self.task_id()
        );
        let _ = self.current_level.remove(&(page_id, l_type));
        let mut opt = self.guard.remove(&(page_id, l_type));
        match &mut opt {
            Some(g) => g.unlock().instrument(info_span!("Guard::unlock")).await,
            None => {
                panic!("error");
            }
        }
    }

    pub async fn allocate_bt_page<C: Compare<[u8]> + 'static>(
        &mut self,
        l_type: PLatchType,
        page_id: PageId,
        level: u32,
        has_high_key: bool,
        pool: &BufPool,
    ) -> Result<PageGuardEP> {
        let init_page = |vec: &mut Vec<u8>| -> Result<()> {
            if level == 0 {
                let mut bt_page = PageBTreeMut::<LEAF_PAGE>::new(vec);
                bt_page.format(page_id, INVALID_PAGE_ID, INVALID_PAGE_ID, level);
                if has_high_key {
                    bt_page.set_high_key_infinite();
                }
            } else {
                let mut bt_page = PageBTreeMut::<NON_LEAF_PAGE>::new(vec);
                bt_page.format(page_id, INVALID_PAGE_ID, INVALID_PAGE_ID, level);
                if has_high_key {
                    bt_page.set_high_key_infinite();
                }
            }

            Ok(())
        };
        let latch: PageLatchEP = pool
            .allocate_empty_page::<_, BtPredCtx<C>, BtPred, BtPredTable>(
                &PageIndex::new(self.file_id, page_id),
                init_page,
            )
            .instrument(info_span!("allocate_page"))
            .await?;

        trace!("lock page {:?}", (page_id, l_type));
        self.add_current_level(page_id, l_type, Some(level));

        let guard = latch.lock(l_type, self.task_id()).await;
        guard.set_level(level).await;
        assert_eq!(guard.latch_type(), PLatchType::WriteLock);
        self.guard.insert((page_id, l_type), guard.clone());
        self.test_lock_order(page_id, level, l_type).await;
        Ok(guard)
    }

    pub async fn release_all(&mut self) {
        for (_, g) in self.guard.iter() {
            g.unlock().instrument(info_span!("Guard::unlock")).await;
        }
        self.guard.clear();
    }

    pub async fn release_non_parent_modification(&mut self) {
        let mut to_released = Vec::new();
        for (id, plt) in self.current_level.iter() {
            if *plt != ParentModification {
                to_released.push((*id, *plt));
            }
        }
        self.current_level.clear();
        for (id, plt) in to_released.iter() {
            self.unlock_page(*id, *plt)
                .instrument(info_span!("unlock_page"))
                .await;
        }
    }

    pub fn reset_current(&mut self) {
        self.opt_debug_level = None;
        self.current_level.clear();
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn test_conflict_location(
        &self,
        debug_context: &mut DebugContext,
        page_id: PageId,
        location: (i64, i64),
        mode: PLatchType,
    ) {
        for (k, (v, lt)) in debug_context
            .location2page
            .range((Bound::Included(location), Bound::Unbounded))
        {
            if *k > location && *v != page_id {
                let opt_page = self.guard.get(&(*v, *lt));
                match opt_page {
                    Some(g) => {
                        let t: PLatchType = g.latch_type();
                        let conflict = PLatchType::conflict(&t, &mode);
                        if conflict && debug_context.task_id != g.task_id() {
                            // if there are any conflict operations,
                            // some errors occur, we mustn't acquire this latch
                            error!(
                                "task id, {} {:?}, ",
                                debug_context.task_id, debug_context.page2location
                            );
                            assert!(false);
                        }
                    }
                    None => {}
                }
            }
        }
        if !debug_context.location2page.contains_key(&location) {
            let o = debug_context
                .location2page
                .insert(location, (page_id, mode));
            assert!(o.is_none());
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn test_lock_order(&mut self, page_id: PageId, page_level: u32, l_type: PLatchType) {
        let mut opt_ctx = None;
        std::mem::swap(&mut self.opt_debug_context, &mut opt_ctx);
        let ctx = match &mut opt_ctx {
            Some(ctx) => ctx,
            None => {
                return;
            }
        };
        self.test_debug_latch(ctx, page_id, page_level, l_type)
            .await;
        self.opt_debug_context = opt_ctx;
    }

    async fn test_debug_latch(
        &self,
        debug_context: &mut DebugContext,
        page_id: PageId,
        page_level: u32,
        l_type: PLatchType,
    ) {
        debug_context.test_elapse();
        let opt = debug_context.page2location.get(&(page_id, l_type));
        let (level, pos) = match opt {
            Some((level, pos)) => (*level, *pos),
            None => {
                let level = -(page_level as i64);
                let opt_pos = debug_context.level2max.get_mut(&level);
                let pos = match opt_pos {
                    Some(max) => {
                        *max += 1;
                        *max
                    }
                    None => {
                        debug_context.level2max.insert(level as i64, 0);
                        0
                    }
                };
                let _ = debug_context
                    .page2location
                    .insert((page_id, l_type), (level, pos));
                let _ = debug_context
                    .location2page
                    .insert((level, pos), (page_id, l_type));
                (level, pos)
            }
        };

        self.test_conflict_location(debug_context, page_id, (level, pos), l_type)
            .await;
    }

    fn add_current_level(&mut self, page_id: PageId, l_type: PLatchType, opt_level: Option<u32>) {
        match self.opt_debug_level {
            None => self.opt_debug_level = opt_level,
            Some(l) => match opt_level {
                Some(level) => {
                    assert_eq!(level, l);
                }
                _ => {}
            },
        }
        self.current_level.insert((page_id, l_type));
    }
}
