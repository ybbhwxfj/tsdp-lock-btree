/// btree file
/// This source implements Lehman and Yao's B-Link Tree using Karl Malbrain's latch protocol.
/// Reference:
///  [Lehman, Yao](https://www.csd.uoc.gr/~hy460/pdf/p650-lehman.pdf)
///  [Karl Malbrain](https://arxiv.org/ftp/arxiv/papers/1009/1009.2764.pdf)
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::fmt::Display;
use std::ops::Bound;
use std::sync::Arc;

use async_recursion::async_recursion;
use json::{array, object, JsonValue};

use tokio::sync::Mutex;
use tracing::{debug, error, info, info_span, trace, trace_span, warn};
use tracing_futures::Instrument;

use adt::compare::Compare;
use adt::range::{RangeBounds, RangeContain};
use adt::slice::{EmptySlice, Slice, SliceRef};
use common::error_type::ET;
use common::id::{gen_oid, OID};
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, TxOption, UpdateOption};
use crate::access::bt_pred::BtPredTable;
use crate::access::bt_pred::{BtPred, BtPredCtx};

use crate::access::buf_pool::BufPool;
use crate::access::cap_of::capacity_of;
use crate::access::const_val::{FileKind, INVALID_PAGE_ID};
use crate::access::cursor::Cursor;
use crate::access::file_id::FileId;
use crate::access::guard_collection::GuardCollection;
use crate::access::handle_page::HandlePage;
use crate::access::handle_read::HandleReadKV;
use crate::access::handle_write::{EmptyHandleWrite, HandleWrite};
use crate::access::latch_type::PLatchType;

use crate::access::modify_list::{ModifyList, ParentModify, ParentModifyOp};
use crate::access::page_bt::{
    bt_key_cmp_empty_as_max, is_leaf_page, LBResult, PageBTree, PageBTreeMut, PageHdrBt,
    BT_PAGE_HEADER_SIZE, LEAF_PAGE, NON_LEAF_PAGE,
};
use crate::access::page_hdr::{PageHdr, PageHdrMut};
use crate::access::page_id::PageId;
use crate::access::page_latch::PageGuardEP;
use crate::access::slot_bt::SlotBt;
use crate::access::space_mgr::SpaceMgr;
use crate::access::to_json::ToJson;

use crate::access::upsert::ResultUpsert;
use crate::access::upsert::Upsert;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum IsCluster {
    Cluster,
    NonCluster,
}

enum SearchParam<R, K> {
    Range(R),
    Key(K),
}

#[derive(Clone, Copy)]
enum NonLeafModify<'a, 'b, OldKey: Slice, NewKey: Slice> {
    NonLeafInsertId(&'a NewKey),
    NonLeafUpdateId(&'a OldKey, &'b NewKey),
}

type RangeOrKey<'a, 'b, K1, K2> = SearchParam<(&'a RangeBounds<K1>, OID), &'b K2>;

pub struct BTreeFile<
    C: Compare<[u8]> + 'static,
    KT: ToJson,
    VT: ToJson,
    const IS_CLU: IsCluster = { IsCluster::Cluster },
> {
    key_cmp: C,
    bt_pred_ctx: BtPredCtx<C>,
    key2json: KT,
    value2json: VT,
    file_id: FileId,
    root_page: Mutex<RootLevel>,
    pool: Arc<BufPool>,
    space_mgr: SpaceMgr,
    enable_debug: bool,
}

pub struct RootLevel {
    id: PageId,
    level: u32,
}

impl Display for Upsert {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RootLevel {
    fn new(id: PageId, level: u32) -> RootLevel {
        RootLevel { id, level }
    }
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, const IS_CLU: IsCluster> Send
    for BTreeFile<C, KT, VT, IS_CLU>
{
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, const IS_CLU: IsCluster> Sync
    for BTreeFile<C, KT, VT, IS_CLU>
{
}

impl<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson, const IS_CLU: IsCluster>
    BTreeFile<C, KT, VT, IS_CLU>
{
    pub fn new(
        key_cmp: C,
        key2json: KT,
        value2json: VT,
        file_id: FileId,
        pool: Arc<BufPool>,
    ) -> Self {
        BTreeFile {
            key_cmp: key_cmp.clone(),
            bt_pred_ctx: BtPredCtx::new(key_cmp.clone()),
            key2json,
            value2json,
            file_id,
            root_page: Mutex::new(RootLevel::new(0, 0)),
            pool,
            space_mgr: SpaceMgr::new(),
            enable_debug: false,
        }
    }

    pub fn enable_debug_latch(self, enable_debug_latch: bool) -> Self {
        let mut s = self;
        s.enable_debug = enable_debug_latch;
        s
    }

    pub fn compare(&self) -> &C {
        &self.key_cmp
    }

    pub async fn get_freed_page_id(&self) -> PageId {
        self.space_mgr.new_page_id().await
    }

    pub async fn open(&self) -> Result<()> {
        let init = |vec: &mut Vec<u8>| {
            let mut page = PageBTreeMut::<LEAF_PAGE>::from(vec);
            page.format(0, INVALID_PAGE_ID, INVALID_PAGE_ID, 0);
            let set_hk_ok = page.set_high_key_infinite();
            if !set_hk_ok {
                return Err(ET::IOError("set high key infinite error".to_string()));
            }
            Ok(())
        };
        let is_empty = |vec: &mut Vec<u8>| {
            let page = PageHdrBt::from(vec);
            Ok(page.get_active_count() == 0)
        };
        self.pool
            .access_path_open::<BtPredTable>(
                self.file_id,
                FileKind::FileBTree,
                &self.space_mgr,
                init,
                is_empty,
            )
            .await
    }

    pub async fn bt_close(&self) {}

    /// key search
    pub async fn search_key<K, F>(
        &self,
        key: &K,
        _: SearchOption,
        handle: &F,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        self.search_gut(key, handle, tx_opt)
            .instrument(trace_span!("bt_search_gut"))
            .await
    }

    /// range search
    pub async fn search_range<K, F>(
        &self,
        range: &RangeBounds<K>,
        _: SearchOption,
        handler: &F,
        tx_opt: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let oid = gen_oid();
        self.search_range_gut(range, oid, handler, tx_opt).await
    }

    /// insert a key/value pair
    pub async fn insert<K, V, H>(
        &self,
        key: &K,
        value: &V,
        opt: InsertOption,
        handle_upsert: &H,
        _tx_opt: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        self.insert_gut(key, value, opt, handle_upsert, _tx_opt)
            .instrument(trace_span!("insert_gut"))
            .await
    }

    /// update a key/value pair
    pub async fn update<K, V, H>(
        &self,
        key: &K,
        value: &V,
        opt: UpdateOption,
        handle_upsert: &H,
        _tx_opt: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        self.update_gut(key, value, opt, handle_upsert, _tx_opt)
            .instrument(trace_span!("insert_gut"))
            .await
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
        self.delete_gut(&self.pool, key, &opt, handle_write, None, tx_opt)
            .instrument(trace_span!("delete_gut"))
            .await
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn insert_gut<K, V, F>(
        &self,
        key: &K,
        value: &V,
        opt: InsertOption,
        h_upsert: &F,
        opt_tx: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        F: HandleWrite,
    {
        let mut guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        debug!("insert task id {}", guards.task_id());
        let max_size = BT_PAGE_HEADER_SIZE
            + SlotBt::<LEAF_PAGE>::slot_size()
            + (key.as_slice().len() + value.as_slice().len()) as u32;
        if (self.pool.page_size() as usize) < (max_size as usize) {
            error!("error size");
            return Err(ET::ExceedCapacity);
        }

        let mut opt_update_root_id = None;
        let r = self
            .update_insert_gut(
                &self.pool,
                key,
                value,
                Upsert::Insert,
                false,
                opt.update_if_exist,
                h_upsert,
                opt_tx,
                &mut guards,
                &mut opt_update_root_id,
            )
            .instrument(trace_span!("update_insert_gut"))
            .await;
        guards
            .release_all()
            .instrument(trace_span!("GuardCollection::release_all"))
            .await;
        self.update_root_page_id(opt_update_root_id)
            .instrument(trace_span!("update_root_page_id"))
            .await;
        r
    }

    pub async fn update_gut<K: Slice, V: Slice, H>(
        &self,
        key: &K,
        value: &V,
        opt: UpdateOption,
        h_upsert: &H,
        opt_tx: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        H: HandleWrite,
    {
        let mut guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);

        debug!("update task id {}", guards.task_id());

        // optional root id and root level
        let mut opt_update_root: Option<(PageId, u32)> = None;

        let r = self
            .update_insert_gut(
                &self.pool,
                key,
                value,
                Upsert::Update,
                opt.insert_if_not_exist,
                false,
                h_upsert,
                opt_tx,
                &mut guards,
                &mut opt_update_root,
            )
            .instrument(trace_span!("bt_update_insert"))
            .await;

        guards
            .release_all()
            .instrument(trace_span!("GuardCollection::release_all"))
            .await;

        self.update_root_page_id(opt_update_root)
            .instrument(trace_span!("update_root_page_id"))
            .await;
        r
    }

    pub async fn to_json(&self) -> Result<JsonValue> {
        let root_id = {
            let root = self.root_page.lock().await;
            root.id
        };

        let mut guard_set = GuardCollection::new(self.file_id);
        let json = self.gut_to_json(root_id, &self.pool, &mut guard_set).await;
        guard_set.release_all().await;
        return json;
    }

    /// remove lock flags on btree page
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
        let mut guard_set =
            GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        debug!("remove lock task {}", guard_set.task_id());

        let r = self
            .remove_lock_flag_gut(read, write, key, opt_cursor, &mut guard_set)
            .instrument(trace_span!("remove_lock_flag_gut"))
            .await;
        guard_set
            .release_all()
            .instrument(trace_span!("GuardCollection::release_all"))
            .await;
        r
    }

    async fn remove_lock_flag_gut<K: Slice>(
        &self,
        read: bool,
        write: bool,
        key: &K,
        opt_cursor: Option<Cursor>,
        guard_set: &mut GuardCollection,
    ) -> Result<()>
    where
        K: Slice,
    {
        let (mut guard, _) = self
            .path_down_with_cursor(
                opt_cursor,
                PLatchType::ReadLock,
                &RangeOrKey::<K, K>::Key(key),
                guard_set,
                &mut None,
            )
            .instrument(trace_span!("path_down_with_cursor"))
            .await?;
        assert_eq!(guard.latch_type(), PLatchType::WriteLock);
        let mut page = PageBTreeMut::<LEAF_PAGE>::new(&mut guard);
        let r = page.lower_bound(key, &self.key_cmp);
        match r {
            LBResult::Found(n, equal) => {
                if equal {
                    let mut slot = page.get_slot_mut(n);
                    if read {
                        slot.clr_read_flag();
                    }
                    if write {
                        slot.clr_write_flag();
                    }
                }
            }
            LBResult::NotFound => {}
        }
        Ok(())
    }

    async fn link_page<const IS_LEAF: bool>(
        &self,
        left: &mut PageGuardEP,
        new: &mut PageGuardEP,
        guard_set: &mut GuardCollection,
    ) -> Result<()> {
        assert_eq!(new.latch_type(), PLatchType::WriteLock);
        let page_new = PageBTreeMut::<IS_LEAF>::from(new);
        let id_right = page_new.get_right_id();
        if id_right != INVALID_PAGE_ID {
            let page_left = PageBTree::<IS_LEAF>::from(left);
            // reset the link of right page
            let mut right = self
                .intent_access_and_upgrade(id_right, PLatchType::WriteLock, None, guard_set)
                .instrument(trace_span!("link page 1, lock page"))
                .await?;

            assert_eq!(right.latch_type(), PLatchType::WriteLock);
            let mut page_right = PageBTreeMut::<IS_LEAF>::from(&mut right);

            assert_eq!(page_new.get_left_id(), page_right.get_left_id());
            assert_eq!(page_left.get_id(), page_right.get_left_id());
            page_right.set_left_id(page_new.get_id());
            trace!("unlock page {:?}", (new.page_id(), new.latch_type()));
        }
        Ok(())
    }
    // Return value:
    // ResultUpsert
    async fn split_and_upsert<const IS_LEAF: bool, K1: Slice, K2: Slice>(
        &self,
        upsert_index: u32,
        upsert: Upsert,
        key: &K1,
        leaf_param: Option<(&K2, &mut Option<TxOption>)>,
        non_leaf_param: Option<PageId>,
        page_guard: &mut PageGuardEP,
        id2update: &mut ModifyList,
        guard_set: &mut GuardCollection,
    ) -> Result<ResultUpsert> {
        let guard = page_guard;
        let current_level = {
            let page_left = PageBTree::<IS_LEAF>::from(guard);
            page_left.get_page_level()
        };
        let page_n_id = self.get_freed_page_id().await;

        let mut guard_new_right = self
            .allocate_page(
                PLatchType::WriteLock,
                page_n_id,
                current_level,
                false,
                guard_set,
            )
            .instrument(trace_span!("allocate_page"))
            .await?;
        guard_new_right
            .merge_pred::<BtPredCtx<C>, BtPred, BtPredTable>(guard)
            .await;

        // before split:
        //      left -> right
        //      left <- right
        // we would insert new between left and write, and we set the links
        //      left.right = new
        //      right.left = new
        //      new.left = left
        //      new.right = right
        // after , we have
        //      left -> new -> right
        //      left <- new <- right

        let result = if IS_LEAF {
            let (value, out) = unsafe { leaf_param.unwrap_unchecked() };
            let r = self
                .leaf_split_upsert_by_index(
                    upsert_index,
                    upsert,
                    key,
                    value,
                    guard,
                    &mut guard_new_right,
                    id2update,
                    out,
                    guard_set,
                )
                .instrument(trace_span!("leaf_split_upsert_by_index"))
                .await?;
            r
        } else {
            let page_id = unsafe { non_leaf_param.unwrap_unchecked() };
            let r = self
                .non_leaf_split_upsert_by_index(
                    upsert_index,
                    upsert,
                    key,
                    page_id,
                    guard,
                    &mut guard_new_right,
                    id2update,
                    guard_set,
                )
                .instrument(trace_span!("non_leaf_split_upsert_by_index"))
                .await?;
            r
        };

        {
            let page = PageBTree::<IS_LEAF>::from(&guard);
            let page_right = PageBTree::<IS_LEAF>::from(&guard_new_right);

            trace!(
                "{}, after split {}: page left : {}",
                guard_set.task_id(),
                upsert,
                page.to_json(&self.key2json, &self.value2json)?.to_string()
            );
            trace!(
                "{}, after split {}: page right : {}",
                guard_set.task_id(),
                upsert,
                page_right
                    .to_json(&self.key2json, &self.value2json)?
                    .to_string()
            );
        }
        Ok(result)
    }

    async fn leaf_upsert_split<K, V>(
        &self,
        page_id: PageId,
        right_link_id: PageId,
        key: &K,
        value: &V,
        upsert: Upsert,
        guard_set: &mut GuardCollection,
        modify_list: &mut ModifyList,
        out: &mut Option<TxOption>,
    ) -> Result<()>
    where
        K: Slice,
        V: Slice,
    {
        let mut opt_id = Some(page_id);
        let mut opt_prev = None;
        while opt_id.is_some() {
            let id = unsafe { opt_id.unwrap_unchecked() };
            let mut guard = self
                .intent_access_and_upgrade(id, PLatchType::WriteLock, opt_prev, guard_set)
                .instrument(trace_span!("leaf_upsert_or_split, lock page"))
                .await?;
            let r1 = {
                assert_eq!(guard.latch_type(), PLatchType::WriteLock);
                let mut page = PageBTreeMut::<LEAF_PAGE>::from(&mut guard);

                let r = page.leaf_search_upsert(
                    key,
                    value,
                    upsert,
                    right_link_id,
                    &self.key_cmp,
                    modify_list,
                    out,
                )?;
                r
            };
            let r2 = match &r1 {
                ResultUpsert::NeedSplit(pos) => {
                    let r = self
                        .split_and_upsert::<LEAF_PAGE, _, _>(
                            *pos,
                            upsert,
                            key,
                            Some((value, out)),
                            None,
                            &mut guard,
                            modify_list,
                            guard_set,
                        )
                        .instrument(trace_span!("split and upsert"))
                        .await?;
                    r
                }
                ResultUpsert::UpsertOk => {
                    if modify_list.contain(guard.page_id()) {
                        let _ = self
                            .lock_page_level(
                                guard.page_id(),
                                PLatchType::ParentModification,
                                guard_set,
                            )
                            .await;
                    }
                    r1
                }
                _ => {
                    panic!("error")
                }
            };
            match r2 {
                ResultUpsert::SearchRight(right) => {
                    opt_id = Some(right);
                }
                ResultUpsert::UpsertOk => {
                    opt_id = None;
                }
                ResultUpsert::NeedSplit(_) => {
                    panic!("error")
                }
                _ => {
                    // search from start
                    error!("search from start");
                    opt_id = Some(page_id);
                }
            }

            opt_prev = Some(guard);
        }
        Ok(())
    }

    async fn leaf_split_upsert_by_index<K: Slice, V: Slice>(
        &self,
        index: u32,
        upsert: Upsert,
        key: &K,
        value: &V,
        page_guard_left: &mut PageGuardEP,
        page_guard_right: &mut PageGuardEP,
        modify_list: &mut ModifyList,
        out: &mut Option<TxOption>,
        guard_set: &mut GuardCollection,
    ) -> Result<ResultUpsert> {
        let mut g_left = page_guard_left;
        let mut g_right = page_guard_right;

        let size = (value.as_slice().len() + key.as_slice().len()) as u32
            + SlotBt::<LEAF_PAGE>::slot_size();
        let cap = capacity_of(size);
        let ok = {
            assert_eq!(g_left.latch_type(), PLatchType::WriteLock);
            assert_eq!(g_right.latch_type(), PLatchType::WriteLock);
            let mut page_left = PageBTreeMut::<LEAF_PAGE>::from(g_left);
            let mut page_right = PageBTreeMut::<LEAF_PAGE>::from(g_right);

            let (split_pos, opt_index, c1) = {
                let c1 = page_left.get_count();
                let (p, i) = page_left.re_organize_and_split_position::<LEAF_PAGE>(index, cap)?;
                (p, i, c1)
            };

            let _ = self.split_page::<LEAF_PAGE>(
                split_pos,
                &mut page_left,
                &mut page_right,
                modify_list,
            )?;

            let c2 = page_left.get_count();
            let c3 = page_right.get_count();
            assert_eq!(c1, c2 + c3);
            let (index, left) = match opt_index {
                Some((index, left)) => (index, left),
                None => {
                    assert!(cap > page_left.get_free_space());
                    assert!(cap > page_right.get_free_space());

                    return Ok(ResultUpsert::SearchFromStart);
                }
            };

            let mut page_to_upsert = if left { page_left } else { page_right };
            let ok =
                page_to_upsert.leaf_upsert_key_value(index, upsert, key, value, modify_list, out);
            debug!(
                "after page upsert, {}",
                page_to_upsert
                    .to_json(&self.key2json, &self.value2json)?
                    .to_string()
            );
            ok
        };
        if !ok {
            error!("add size {}, index:{}", cap, index);
            panic!("TODO")
        }
        self.link_page::<LEAF_PAGE>(&mut g_left, &mut g_right, guard_set)
            .await?;
        if modify_list.contain(g_left.page_id()) {
            let _ = self
                .lock_page_level(g_left.page_id(), PLatchType::ParentModification, guard_set)
                .await;
        }
        if modify_list.contain(g_right.page_id()) {
            let _ = self
                .lock_page_level(g_right.page_id(), PLatchType::ParentModification, guard_set)
                .await;
        }

        Ok(ResultUpsert::UpsertOk)
    }

    async fn non_leaf_split_upsert_by_index<K: Slice>(
        &self,
        index: u32,
        upsert: Upsert,
        key: &K,
        child_page_id: PageId,
        left_page_guard: &mut PageGuardEP,
        right_page_guard: &mut PageGuardEP,
        modify_list: &mut ModifyList,
        guard_set: &mut GuardCollection,
    ) -> Result<ResultUpsert> {
        assert_ne!(child_page_id, INVALID_PAGE_ID);

        let size = key.as_slice().len() as u32 + SlotBt::<NON_LEAF_PAGE>::slot_size();
        let g_left = left_page_guard;
        let g_right = right_page_guard;

        let ok = {
            assert_eq!(g_left.latch_type(), PLatchType::WriteLock);
            assert_eq!(g_right.latch_type(), PLatchType::WriteLock);
            let mut page_left = PageBTreeMut::<NON_LEAF_PAGE>::from(g_left);
            let mut page_right = PageBTreeMut::<NON_LEAF_PAGE>::from(g_right);
            let (split_pos, opt_index, c1) = {
                let c1 = page_left.get_count();
                let (p, i) =
                    page_left.re_organize_and_split_position::<NON_LEAF_PAGE>(index, size)?;
                (p, i, c1)
            };

            let _ = self.split_page::<NON_LEAF_PAGE>(
                split_pos,
                &mut page_left,
                &mut page_right,
                modify_list,
            )?;

            //trace!("after split non leaf, left: {}", page.to_json(&self.key_desc, &self.value_desc).unwrap().to_string());
            //trace!("after split non leaf, right: {}", page_n.to_json(&self.key_desc, &self.value_desc).unwrap().to_string());
            let c2 = page_left.get_count();
            let c3 = page_right.get_count();
            assert_eq!(c1, c2 + c3);
            let (index, left) = match opt_index {
                Some((index, left)) => (index, left),
                None => {
                    return Ok(ResultUpsert::SearchFromStart);
                }
            };
            let mut page = if left { page_left } else { page_right };
            let ok = self.non_leaf_upsert_key_id(
                index,
                upsert,
                key,
                child_page_id,
                &mut page,
                modify_list,
            );
            ok
        };
        if !ok {
            error!("add size {}, index:{}", size, index);
            panic!("TODO, cannot happen")
        }
        self.link_page::<NON_LEAF_PAGE>(g_left, g_right, guard_set)
            .instrument(trace_span!("link_page"))
            .await?;
        Ok(ResultUpsert::UpsertOk)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    fn non_leaf_upsert_key_id<K: Slice>(
        &self,
        index: u32,
        upsert: Upsert,
        key: &K,
        page_id: PageId,
        page: &mut PageBTreeMut<NON_LEAF_PAGE>,
        modify_list: &mut ModifyList,
    ) -> bool {
        let ret = match upsert {
            Upsert::Update => page.non_leaf_update_index(index, key, page_id, modify_list),
            Upsert::Insert => page.non_leaf_insert_index(index, key, page_id, modify_list),
        };
        ret
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn non_leaf_upsert_or_split<K1, K2, K3>(
        &self,
        page_guard: &mut PageGuardEP,
        upsert: &NonLeafModify<'_, '_, K1, K2>,
        parent_high_key: &K3,
        child_page_id: PageId,
        right_link_id: PageId,
        modify_list: &mut ModifyList,
        guard_set: &mut GuardCollection,
    ) -> Result<ResultUpsert>
    where
        K1: Slice,
        K2: Slice,
        K3: Slice,
    {
        let guard = page_guard;
        assert_ne!(child_page_id, INVALID_PAGE_ID);
        let mut upsert_kind: Upsert;
        let result = match *upsert {
            NonLeafModify::NonLeafInsertId(child_new_high_key) => {
                upsert_kind = Upsert::Insert;
                assert_eq!(guard.latch_type(), PLatchType::WriteLock);
                let mut bt_page = PageBTreeMut::<NON_LEAF_PAGE>::from(guard);
                bt_page.non_leaf_search_and_insert_child(
                    child_new_high_key,
                    parent_high_key,
                    child_page_id,
                    right_link_id,
                    &self.key_cmp,
                    modify_list,
                )?
            }
            NonLeafModify::NonLeafUpdateId(child_old_high_key, child_new_high_key) => {
                upsert_kind = Upsert::Update;
                assert_eq!(guard.latch_type(), PLatchType::WriteLock);
                let mut bt_page = PageBTreeMut::<NON_LEAF_PAGE>::from(guard);
                let r = bt_page.non_leaf_search_and_update_key(
                    child_new_high_key,
                    child_old_high_key,
                    child_page_id,
                    &self.key_cmp,
                    modify_list,
                );
                match &r {
                    Ok(res) => {
                        {
                            let page = PageBTree::<NON_LEAF_PAGE>::from(&guard);
                            trace!(
                                "{}, after {} : page : {}",
                                guard_set.task_id(),
                                upsert_kind,
                                page.to_json(&self.key2json, &self.value2json)?.to_string()
                            );
                        }
                        res.clone()
                    }
                    Err(e) => {
                        return if *e == ET::PageUpdateErrorHighKeySlot {
                            error!("modify list , {:?}, ", modify_list);
                            error!(
                                "find key old key {}, {}, new key {}",
                                child_page_id,
                                self.key2json
                                    .to_json(child_old_high_key.as_slice())
                                    .unwrap()
                                    .to_string(),
                                self.key2json
                                    .to_json(child_new_high_key.as_slice())
                                    .unwrap()
                                    .to_string()
                            );
                            error!(
                                "page, {}",
                                bt_page
                                    .to_json(&self.key2json, &self.value2json)
                                    .unwrap()
                                    .to_string()
                            );
                            r
                        } else {
                            r
                        }
                    }
                }
            }
        };
        let ret = match &result {
            ResultUpsert::NeedSplit(pos) => {
                let (key, insert_or_update) = match *upsert {
                    NonLeafModify::NonLeafInsertId(k) => (k, Upsert::Insert),
                    NonLeafModify::NonLeafUpdateId(_, k) => (k, Upsert::Update),
                };

                let r = self
                    .split_and_upsert::<NON_LEAF_PAGE, _, EmptySlice>(
                        *pos,
                        insert_or_update,
                        key,
                        None,
                        Some(child_page_id),
                        guard,
                        modify_list,
                        guard_set,
                    )
                    .instrument(trace_span!("split_and_upsert"))
                    .await?;
                Ok(r)
            }
            ResultUpsert::UpsertOk => Ok(result),
            _ => {
                return Ok(result);
            }
        };
        ret
    }

    async fn allocate_page(
        &self,
        l_type: PLatchType,
        page_id: PageId,
        level: u32,
        has_high_key: bool,
        guard_set: &mut GuardCollection,
    ) -> Result<PageGuardEP> {
        guard_set
            .allocate_bt_page::<C>(l_type, page_id, level, has_high_key, &self.pool)
            .instrument(trace_span!("allocate_bt_page"))
            .await
    }

    async fn create_new_parent(
        &self,
        l_type: PLatchType,
        current_level: u32,
        guards: &mut GuardCollection,
    ) -> Result<PageGuardEP> {
        if current_level > 2 {
            warn!("current b-tree height is {}", (current_level + 1));
        };

        let id = self.get_freed_page_id().await;
        let guard = self
            .allocate_page(l_type, id, current_level, false, guards)
            .instrument(trace_span!("allocate_bt_page"))
            .await?;
        trace!("tree level increase, level:{}, id:{}", current_level, id);
        Ok(guard)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn non_leaf_upsert_key_level<K1: Slice, K2: Slice, K3: Slice>(
        &self,
        page_id: PageId,
        upsert: &NonLeafModify<'_, '_, K1, K2>,
        release_previous: bool,
        parent_high_key: &K3,
        right_link_id: PageId,
        opt_parent_page_id: Option<PageId>,
        update_set: &mut ModifyList,
        guards: &mut GuardCollection,
    ) -> Result<()> {
        let mut next_page_id = opt_parent_page_id;
        let mut opt_release = None;
        while next_page_id.is_some() {
            let p_id = unsafe { next_page_id.unwrap_unchecked() };
            let mut page_guard = self
                .intent_access_and_upgrade(p_id, PLatchType::WriteLock, opt_release, guards)
                .await?;

            // split the page and update high key
            let result = self
                .non_leaf_upsert_or_split(
                    &mut page_guard,
                    upsert,
                    parent_high_key,
                    page_id,
                    right_link_id,
                    update_set,
                    guards,
                )
                .instrument(trace_span!("non_leaf_upsert_or_split"))
                .await?;

            match result {
                ResultUpsert::SearchRight(right_id) => {
                    next_page_id = Some(right_id);
                }
                ResultUpsert::UpsertOk => {
                    // stop this loop
                    next_page_id = None;
                }
                ResultUpsert::NonExistingKey => {
                    return Err(ET::PageUpdateErrorNoSuchKey);
                }
                ResultUpsert::ExistingKey => {
                    return Err(ET::PageUpdateErrorExistingSuchKey);
                }
                ResultUpsert::SearchFromStart => {
                    // loop from started
                    next_page_id = opt_parent_page_id;
                }
                _ => {
                    panic!("error")
                }
            }

            opt_release = if release_previous {
                Some(page_guard)
            } else {
                None
            };
        }
        Ok(())
    }

    async fn modify_non_leaf_page(
        &self,
        pool: &BufPool,
        to_modify_id: PageId,
        new_parent: bool,
        modify_list: &mut Vec<ParentModify>,
        next_level_modify: &mut ModifyList,
        guards: &mut GuardCollection,
        removed_left: &mut HashSet<(PageId, PageId)>,
    ) -> Result<()> {
        let parent_id = to_modify_id;
        let opt_parent_page_id = Some(parent_id);
        let (parent_high_key, right_link_id, guard) = {
            let page_guard = self
                .intent_access_and_upgrade(parent_id, PLatchType::WriteLock, None, guards)
                .instrument(trace_span!("lock_page_level"))
                .await?;
            if is_leaf_page(&page_guard) {
                let page = PageBTree::<LEAF_PAGE>::from(&page_guard);
                let key = match page.get_last_key() {
                    Some(v) => Vec::from(v),
                    None => Vec::new(),
                };
                (key, page.get_right_id(), page_guard)
            } else {
                let page = PageBTree::<NON_LEAF_PAGE>::from(&page_guard);
                let key = match page.get_last_key() {
                    Some(v) => Vec::from(v),
                    None => Vec::new(),
                };
                (key, page.get_right_id(), page_guard)
            }
        };
        // to keep this procedure atomic, we would not release previous latch when accessing
        // next node

        loop {
            let opt = modify_list.last();
            let modify = match opt {
                Some(modify) => modify,
                None => {
                    break;
                }
            };

            trace!(
                "{},  update parent : {}",
                guards.task_id(),
                ModifyList::op_to_json(modify, &self.key2json)?.to_string()
            );

            match &modify.modify_op {
                ParentModifyOp::ParentInsertSub => {
                    let high_key = match &modify.new_high_key {
                        Some(v) => v.clone(),
                        None => {
                            panic!("error")
                        }
                    };
                    let result_insert = self
                        .non_leaf_upsert_key_level(
                            modify.child_id,
                            &NonLeafModify::NonLeafInsertId::<&[u8], _>(&high_key),
                            false,
                            &parent_high_key,
                            right_link_id,
                            opt_parent_page_id,
                            next_level_modify,
                            guards,
                        )
                        .await;
                    if result_insert.is_err() {
                        panic!("error");
                    }
                }
                ParentModifyOp::ParentUpdateSub => {
                    let new_high_key = match &modify.new_high_key {
                        Some(v) => v.clone(),
                        None => {
                            panic!("error")
                        }
                    };
                    let old_high_key = match &modify.old_high_key {
                        Some(v) => v.clone(),
                        None => {
                            panic!("error")
                        }
                    };
                    let result_update = self
                        .non_leaf_upsert_key_level(
                            modify.child_id,
                            &NonLeafModify::NonLeafUpdateId(&old_high_key, &new_high_key),
                            false,
                            &parent_high_key,
                            right_link_id,
                            opt_parent_page_id,
                            next_level_modify,
                            guards,
                        )
                        // .instrument(trace_span!("non_leaf_upsert_key_level"))
                        .await;
                    match &result_update {
                        Ok(_) => {}
                        Err(e) => {
                            match *e {
                                ET::PageUpdateErrorHighKeySlot => {
                                    // first update, and then insert
                                    let result_remove = self
                                        .remove_key_level::<NON_LEAF_PAGE, _, EmptyHandleWrite>(
                                            pool,
                                            guard.clone(),
                                            modify.child_id,
                                            &old_high_key,
                                            true,
                                            false,
                                            None,
                                            guards,
                                            next_level_modify,
                                            removed_left,
                                        )
                                        //.instrument(trace_span!("remove_key_level"))
                                        .await;
                                    if result_remove.is_err() {
                                        panic!("error")
                                    }
                                    let result_insert = self
                                        .non_leaf_upsert_key_level(
                                            modify.child_id,
                                            &NonLeafModify::NonLeafInsertId::<&[u8], _>(
                                                &new_high_key,
                                            ),
                                            false,
                                            &parent_high_key,
                                            right_link_id,
                                            opt_parent_page_id,
                                            next_level_modify,
                                            guards,
                                        )
                                        .await;
                                    if result_insert.is_err() {
                                        panic!("error");
                                    }
                                }
                                ET::PageUpdateErrorNoSuchKey => {
                                    if new_parent {
                                        let result_insert = self
                                            .non_leaf_upsert_key_level(
                                                modify.child_id,
                                                &NonLeafModify::NonLeafInsertId::<&[u8], _>(
                                                    &new_high_key,
                                                ),
                                                false,
                                                &parent_high_key,
                                                right_link_id,
                                                opt_parent_page_id,
                                                next_level_modify,
                                                guards,
                                            )
                                            .await;
                                        if result_insert.is_err() {
                                            panic!("error");
                                        }
                                    } else {
                                        return result_update;
                                    }
                                }
                                _ => {
                                    return result_update;
                                }
                            }
                        }
                    }
                }
                ParentModifyOp::ParentDeleteSub => {
                    let old_high_key = match &modify.old_high_key {
                        Some(v) => v.clone(),
                        None => {
                            panic!("error")
                        }
                    };
                    let result_remove = self
                        .remove_key_level::<NON_LEAF_PAGE, _, EmptyHandleWrite>(
                            pool,
                            guard.clone(),
                            modify.child_id,
                            &old_high_key,
                            true,
                            false,
                            None,
                            guards,
                            next_level_modify,
                            removed_left,
                        )
                        //.instrument(trace_span!("remove_key_level"))
                        .await;
                    if !new_parent {
                        error!(
                            "cannot find key:{}",
                            self.key2json
                                .to_json(old_high_key.as_slice())
                                .unwrap()
                                .to_string()
                        );
                        let p = PageBTree::<NON_LEAF_PAGE>::from(&guard);
                        error!(
                            "in page {}",
                            p.to_json(&self.key2json, &self.value2json)
                                .unwrap()
                                .to_string()
                        );
                        assert!(result_remove.is_ok());
                    }
                }
            }
            {
                trace!(
                    "next level modify update parent {}",
                    next_level_modify.to_json(&self.key2json)?.to_string()
                );
            }

            guards
                .unlock_page(modify.child_id, PLatchType::ParentModification)
                .instrument(trace_span!("unlock_page, ParentModification"))
                .await;

            for id in &next_level_modify.modify_page_ids() {
                let _ = self
                    .lock_page_level(*id, PLatchType::ParentModification, guards)
                    .instrument(trace_span!("lock parent ParentModification"))
                    .await;
            }

            let _ = modify_list.pop();
        }

        Ok(())
    }

    async fn non_leaf_modify_bottom_up(
        &self,
        pool: &BufPool,
        stack: &mut VecDeque<(PageId, u32)>,
        guards: &mut GuardCollection,
        list: ModifyList,
        removed_left: &mut HashSet<(PageId, PageId)>,
    ) -> Result<Option<(PageId, u32)>> {
        let mut opt_root_page_id = None;
        let mut current_level = 0;
        let mut modify_list = list;

        guards
            .release_non_parent_modification()
            .instrument(trace_span!("release leaf lock"))
            .await;
        guards.reset_current();

        while !modify_list.is_empty() {
            let opt_id = match stack.pop_back() {
                Some((id, level)) => {
                    assert_ne!(level, 0); // do not a leaf node
                    let _ = self
                        .intent_access_and_upgrade(id, PLatchType::WriteLock, None, guards)
                        .instrument(trace_span!("lock page level"))
                        .await?;
                    Some(id)
                }
                None => None,
            };
            current_level += 1;
            let opt_parent_page_id = opt_id;
            let mut new_parent = false;
            let modify_page_id = match opt_parent_page_id {
                Some(id) => id,
                None => {
                    if modify_list.num_new_page() == 0 {
                        // non leaf page split
                        break;
                    } else {
                        // create new parent...
                        new_parent = true;
                        let guard = self
                            .create_new_parent(PLatchType::WriteLock, current_level, guards)
                            .instrument(trace_span!("create_new_parent"))
                            .await?;
                        let root_id = guard.page_id();

                        match &mut opt_root_page_id {
                            Some((id, level)) => {
                                if *level < current_level {
                                    *id = root_id;
                                    *level = current_level;
                                }
                            }
                            None => {
                                opt_root_page_id = Some((root_id, current_level));
                            }
                        }

                        root_id
                    }
                }
            };

            trace!(
                "update non leaf {} {}",
                guards.task_id(),
                modify_list.to_json(&self.key2json)?.to_string()
            );
            let mut this_level_modify = ModifyList::new();
            let mut vec = modify_list.move_to_list();

            self.modify_non_leaf_page(
                pool,
                modify_page_id,
                new_parent,
                &mut vec,
                &mut this_level_modify,
                guards,
                removed_left,
            )
            .instrument(trace_span!("modify non leaf page"))
            .await?;
            // release all locks ont this level
            guards
                .release_non_parent_modification()
                .instrument(trace_span!("release_level", "{}", line!()))
                .await;
            guards.reset_current();
            modify_list = this_level_modify;
        }
        guards.release_non_parent_modification().await;
        Ok(opt_root_page_id)
    }

    /// split the page and update high key from bottom to up
    /// (from the lowest non-leaf node to root node)
    /// return:
    ///     optional update root id
    #[cfg_attr(debug_assertions, inline(never))]
    async fn upsert_leaf_and_modify_bottom_up<K, V>(
        &self,
        pool: &BufPool,
        key: &K,
        value: &V,
        upsert: Upsert,
        leaf_id: PageId,
        right_link_id: PageId,
        stack: &mut VecDeque<(PageId, u32)>,
        guards: &mut GuardCollection,
        update_set: ModifyList,
        removed_left: &mut HashSet<(PageId, PageId)>,
        out: &mut Option<TxOption>,
    ) -> Result<Option<(PageId, u32)>>
    where
        K: Slice,
        V: Slice,
    {
        let mut modify_list = update_set;
        match stack.back() {
            Some((_id, level)) => {
                if *level == 0 {
                    stack.pop_back();
                }
            }
            None => {}
        }

        self.leaf_upsert_split(
            leaf_id,
            right_link_id,
            key,
            value,
            upsert,
            guards,
            &mut modify_list,
            out,
        )
        .instrument(trace_span!("leaf upsert or split"))
        .await?;

        let ret = self
            .non_leaf_modify_bottom_up(pool, stack, guards, modify_list, removed_left)
            .instrument(trace_span!("non_leaf_modify_bottom_up"))
            .await?;
        Ok(ret)
    }

    async fn delete_gut<K, H>(
        &self,
        pool: &BufPool,
        key: &K,
        opt: &DeleteOption,
        handle_write: &H,
        opt_cursor: Option<Cursor>,
        out: &mut Option<TxOption>,
    ) -> Result<bool>
    where
        K: Slice,
        H: HandleWrite,
    {
        //debug!("delete gut: {}", self.key2json.to_json(key.as_slice()).unwrap().to_string());

        let mut guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        debug!("delete key task id {}", guards.task_id());

        // (left_page_id , to_be_removed_page_id)
        let mut removed_left = HashSet::new();
        let (guard, mut stack) = self
            .path_down_with_cursor::<EmptySlice, K>(
                opt_cursor,
                PLatchType::WriteLock,
                &RangeOrKey::Key(key),
                &mut guards,
                out,
            )
            .instrument(trace_span!("delete path_down"))
            .await?;

        let ret = self
            .remove_and_modify_bottom_up(
                pool,
                key,
                guard,
                opt.remove_key,
                handle_write,
                &mut stack,
                &mut guards,
                &mut removed_left,
            )
            .instrument(trace_span!("remove_bottom_up"))
            .await;
        guards.release_all().await;

        guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        // debug!("delete gut: {}, ok", self.key2json.to_json(key.as_slice()).unwrap().to_string());
        if ret.is_ok() {
            let _ = self
                .remove_page(&mut removed_left, &mut guards)
                .instrument(trace_span!("remove page"))
                .await;
            guards.release_all().await;
        }
        ret
    }

    async fn remove_page(
        &self,
        removed_left: &mut HashSet<(PageId, PageId)>,
        guards: &mut GuardCollection,
    ) -> Result<()> {
        for (i1, i2) in removed_left.iter() {
            guards.reset_current();
            let left_id = *i1;
            let remove_id = *i2;
            if left_id != INVALID_PAGE_ID {
                let mut g1 = self
                    .intent_access_and_upgrade(left_id, PLatchType::WriteLock, None, guards)
                    .instrument(trace_span!("remove page 3, lock page"))
                    .await?;

                let next_id = {
                    let left = PageHdr::from(&g1);
                    let next_id = left.get_right_id();
                    next_id
                };

                if next_id != INVALID_PAGE_ID {
                    let mut left = PageHdrMut::from(&mut g1);

                    let page_to_remove = self
                        .intent_access_and_upgrade(next_id, PLatchType::WriteLock, None, guards)
                        .instrument(trace_span!("lock page"))
                        .await?;

                    let (is_empty, right_id) = {
                        let hdr = PageHdrBt::from(&page_to_remove);
                        (hdr.get_count() == 0, hdr.get_right_id())
                    };

                    if is_empty {
                        if right_id != INVALID_PAGE_ID {
                            let mut g3 = self
                                .intent_access_and_upgrade(
                                    right_id,
                                    PLatchType::WriteLock,
                                    None,
                                    guards,
                                )
                                .instrument(trace_span!("remove page 2, lock page"))
                                .await?;
                            let mut right = PageHdrMut::from(&mut g3);
                            left.set_right_id(right_id);
                            right.set_left_id(left_id);
                        } else {
                            left.set_right_id(right_id);
                            assert_eq!(left.get_right_id(), right_id);
                        }
                        let _ = self
                            .lock_page_level(
                                page_to_remove.page_id(),
                                PLatchType::NodeDelete,
                                guards,
                            )
                            .await;
                        self.mark_page_removed(next_id).await;
                    }
                }
            } else if remove_id != INVALID_PAGE_ID {
                assert_eq!(left_id, INVALID_PAGE_ID);
                let page_to_remove = self
                    .intent_access_and_upgrade(remove_id, PLatchType::WriteLock, None, guards)
                    .instrument(trace_span!("remove_page2 -> lock page"))
                    .await?;
                let mid = PageHdr::from(&page_to_remove);
                let right_id = mid.get_right_id();
                if right_id != INVALID_PAGE_ID {
                    let mut g = self
                        .intent_access_and_upgrade(right_id, PLatchType::WriteLock, None, guards)
                        .instrument(trace_span!("remove page, lock page"))
                        .await?;
                    let mut right = PageHdrMut::from(&mut g);
                    right.set_left_id(left_id);
                    assert_eq!(right.get_left_id(), left_id);
                }
                let _ = self
                    .lock_page_level(page_to_remove.page_id(), PLatchType::NodeDelete, guards)
                    .await;
                // mark_this page removed
                self.mark_page_removed(remove_id).await;
            }
            guards.reset_current();
            guards.release_all().await;
        }
        Ok(())
    }
    async fn remove_and_modify_bottom_up<K, H>(
        &self,
        pool: &BufPool,
        key: &K,
        page_guard: PageGuardEP,
        remove_key: bool,
        handle_write: &H,
        stack: &mut VecDeque<(PageId, u32)>,
        guards: &mut GuardCollection,
        removed_left: &mut HashSet<(PageId, PageId)>,
    ) -> Result<bool>
    where
        K: Slice,
        H: HandleWrite,
    {
        match stack.back() {
            Some((_id, level)) => {
                if *level == 0 {
                    // erase leaf page
                    stack.pop_back();
                }
            }
            None => {}
        }

        let mut ops = ModifyList::new();
        let result = self
            .remove_key_level::<LEAF_PAGE, _, _>(
                pool,
                page_guard,
                INVALID_PAGE_ID,
                key,
                remove_key,
                true,
                Some(handle_write),
                guards,
                &mut ops,
                removed_left,
            )
            .instrument(trace_span!("remove_key_level"))
            .await;
        return match result {
            Ok(()) => {
                self.non_leaf_modify_bottom_up(pool, stack, guards, ops, removed_left)
                    .instrument(trace_span!("non_leaf_modify_bottom_up"))
                    .await?;
                Ok(true)
            }
            Err(e) => {
                if e == ET::PageUpdateErrorNoSuchKey {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        };
    }

    /// remove key in the same node level
    async fn remove_key_level<const LEAF: bool, K, H>(
        &self,
        pool: &BufPool,
        page_guard: PageGuardEP,
        slot_page_id: PageId, // only invalid for non leaf page
        key: &K,
        remove_key: bool,
        release_previous: bool,
        handle_write: Option<&H>,
        guards: &mut GuardCollection,
        ops: &mut ModifyList,
        removed_left: &mut HashSet<(PageId, PageId)>,
    ) -> Result<()>
    where
        K: Slice,
        H: HandleWrite,
    {
        assert!(
            (LEAF && slot_page_id == INVALID_PAGE_ID) || (!LEAF && slot_page_id != INVALID_PAGE_ID)
        );
        let mut opt_page = Some(page_guard.page_id());
        let mut opt_prev = None;
        while opt_page.is_some() {
            let id = unsafe { opt_page.unwrap_unchecked() };
            let mut guard = self
                .intent_access_and_upgrade(id, PLatchType::WriteLock, opt_prev, guards)
                .await?;

            let ret = self
                .remove_key_from_page::<LEAF, _, _>(
                    pool,
                    &mut guard,
                    slot_page_id,
                    key,
                    remove_key,
                    handle_write,
                    guards,
                    ops,
                    removed_left,
                )
                .instrument(trace_span!("remove_key_from_page"))
                .await;
            if ret.is_ok() {
                let page = PageBTree::<LEAF>::from(&page_guard);
                trace!(
                    "{}, after remove key {} : page : {}",
                    guards.task_id(),
                    self.key2json.to_json(key.as_slice())?.to_string(),
                    page.to_json(&self.key2json, &self.value2json)?.to_string()
                );
            }
            opt_prev = if release_previous { Some(guard) } else { None };

            match ret {
                Ok(v) => {
                    opt_page = v;
                }
                Err(e) => {
                    return if e == ET::PageUpdateErrorNoSuchKey {
                        Err(ET::PageUpdateErrorNoSuchKey)
                    } else {
                        Err(e)
                    };
                }
            }
        }
        Ok(())
    }

    async fn mark_page_removed(&self, _page_id: PageId) {
        // todo fixme reuse removed page id
        // self.space_mgr.page_removed(_page_id).await
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn remove_key_from_page<const LEAF: bool, K, H>(
        &self,
        _pool: &BufPool,
        guard: &mut PageGuardEP,
        page_id: PageId, // valid for non leaf_page
        key: &K,
        remove_key: bool,
        handle_write: Option<&H>,
        guards: &mut GuardCollection,
        ops: &mut ModifyList,
        removed_left: &mut HashSet<(PageId, PageId)>,
    ) -> Result<Option<PageId>>
    where
        K: Slice,
        H: HandleWrite,
    {
        let (index, count, left_id) = {
            let page = PageBTree::<LEAF>::from(guard);

            let lb_result = page.lower_bound::<K, C>(key, &self.key_cmp);
            let index = match lb_result {
                LBResult::Found(index, is_equal) => {
                    if !is_equal {
                        guards
                            .unlock_page(guard.page_id(), guard.latch_type())
                            .instrument(trace_span!("unlock_page"))
                            .await;

                        return Err(ET::PageUpdateErrorNoSuchKey);
                    }
                    index
                }
                LBResult::NotFound => {
                    let next_page_id = page.get_right_id();
                    return if next_page_id != INVALID_PAGE_ID {
                        Ok(Some(next_page_id))
                    } else {
                        Err(ET::PageUpdateErrorNoSuchKey)
                    };
                }
            };
            if !LEAF {
                let slot = page.get_slot(index);
                if slot.get_page_id() != page_id {
                    error!(
                        "page: {}",
                        page.to_json(&self.key2json, &self.value2json)?.to_string()
                    );
                    error!(
                        "index: {} key:{}",
                        index,
                        self.key2json.to_json(key.as_slice())?.to_string()
                    );
                    panic!("cannot happen");
                }
            } else {
                assert_eq!(page_id, INVALID_PAGE_ID);
            }
            (index, page.get_count(), page.get_left_id())
        };
        match handle_write {
            Some(h) => {
                let page = PageBTree::<LEAF>::from(guard);
                let value = page.get_value(index);
                h.on_delete(value).await?;
            }
            None => {}
        }
        if remove_key {
            if count > 1 {
                assert_eq!(guard.latch_type(), PLatchType::WriteLock);
                let mut page = PageBTreeMut::<LEAF>::from(guard);
                page.remove(index, ops);
            } else {
                // this page would be empty add left link id of the page to removed_left,
                // for later recycle and reset link pointer
                removed_left.insert((left_id, guard.page_id()));
                assert_eq!(guard.latch_type(), PLatchType::WriteLock);
                let mut page = PageBTreeMut::<LEAF>::from(guard);
                page.remove(index, ops);
            }

            if ops.contain(guard.page_id()) {
                let _ = self
                    .lock_page_level(guard.page_id(), PLatchType::ParentModification, guards)
                    .await;
            }
        }
        Ok(None)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn update_insert_gut<K, V, H>(
        &self,
        pool: &BufPool,
        key: &K,
        value: &V,
        upsert: Upsert,
        insert_if_not_exist: bool,
        update_if_exist: bool,
        h_upsert: &H,
        tx_opt: &mut Option<TxOption>,
        guards: &mut GuardCollection,
        opt_update_root: &mut Option<(PageId, u32)>,
    ) -> Result<bool>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite,
    {
        // debug!("insert key {}", self.key2json.to_json(key.as_slice()).unwrap().to_string());
        let (mut guard, mut stack) = self
            .path_down_with_cursor::<EmptySlice, _>(
                None,
                PLatchType::WriteLock,
                &RangeOrKey::Key(key),
                guards,
                tx_opt,
            )
            .instrument(trace_span!("path_down_with_cursor"))
            .await?;

        let (index, found) = {
            let page = PageBTree::<LEAF_PAGE>::from(&mut guard);
            let lb_result = page.lower_bound::<K, C>(key, &self.key_cmp);
            let (index, found) = match lb_result {
                LBResult::Found(index, is_equal) => (index, is_equal),
                LBResult::NotFound => {
                    error!(
                        "cannot find key:{}",
                        self.key2json.to_json(key.as_slice()).unwrap().to_string()
                    );
                    error!(
                        "in page {}",
                        page.to_json(&self.key2json, &self.value2json)
                            .unwrap()
                            .to_string()
                    );
                    panic!("TODO")
                }
            };
            (index, found)
        };
        let opt_upsert = match upsert {
            Upsert::Update => {
                if found {
                    Some(Upsert::Update)
                } else {
                    if insert_if_not_exist {
                        Some(Upsert::Insert)
                    } else {
                        None
                    }
                }
            }
            Upsert::Insert => {
                if found {
                    if update_if_exist {
                        Some(Upsert::Update)
                    } else {
                        None
                    }
                } else {
                    Some(Upsert::Insert)
                }
            }
        };
        let update_or_insert = match opt_upsert {
            None => {
                return Ok(false);
            }
            Some(v) => v,
        };
        let r = match update_or_insert {
            Upsert::Update => {
                let page = PageBTree::<LEAF_PAGE>::from(&mut guard);
                match tx_opt {
                    Some(opt) => {
                        let slot = page.get_slot(index);
                        if slot.is_read_locked() {
                            let conflict = opt.add_conflict_key(Vec::from(key.as_slice()));
                            if conflict {
                                let page_id = page.get_id();
                                let seq_no = page.get_seq_no();
                                let cursor = opt.mutable_cursor();
                                cursor.set_page_id(page_id);
                                cursor.set_slot_no(index);
                                cursor.set_seq(seq_no);
                                return Err(ET::TxConflict);
                            }
                        }
                    }
                    _ => {}
                }

                let pointer = page.get_value(index);
                h_upsert.on_update(value.as_slice(), pointer).await
            }
            Upsert::Insert => h_upsert.on_insert(value.as_slice()).await,
        };

        let opt_pointer = match r {
            Ok(v) => v,
            Err(e) => {
                error!("upsert error");
                return Err(e);
            }
        };

        let insert_value = match IS_CLU {
            IsCluster::Cluster => value.as_slice(),
            IsCluster::NonCluster => {
                match &opt_pointer {
                    Some(v) => v.as_slice(),
                    None => {
                        // for a cluster index, we only modify tuple in heap file if necessary
                        return Ok(true);
                    }
                }
            }
        };

        let mut list = ModifyList::new();
        let opt_guard = self
            .leaf_upsert_key_value(
                guard,
                index,
                update_or_insert,
                key,
                &SliceRef::new(insert_value),
                &mut list,
                tx_opt,
                guards,
            )
            .instrument(trace_span!("leaf_upsert_key_value"))
            .await;

        match opt_guard {
            Some(mut g) => {
                // update/insert failed, for lack of available space in this pages,
                // we need to split this page
                let mut removed_left = HashSet::new();
                let page = PageBTree::<LEAF_PAGE>::from(&mut g);
                let leaf_page_id = page.get_id();
                // we would do not try to acquire latch of the right link page
                let right_link_id = page.get_right_id();

                let r = self
                    .upsert_leaf_and_modify_bottom_up(
                        pool,
                        key,
                        &SliceRef::new(insert_value),
                        update_or_insert,
                        leaf_page_id,
                        right_link_id,
                        &mut stack,
                        guards,
                        list,
                        &mut removed_left,
                        tx_opt,
                    )
                    .instrument(trace_span!("bt_split_bottom_up"))
                    .await;

                *opt_update_root = match r {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(e);
                    }
                };
                Ok(true)
            }
            None => Ok(true),
        }
    }

    /// leaf update/insert key value pairs
    /// if succeed, return None
    /// else, return this leaf page guard
    async fn leaf_upsert_key_value<K: Slice, V: Slice>(
        &self,
        page_guard: PageGuardEP,
        index: u32,
        upsert: Upsert,
        key: &K,
        value: &V,
        modify_list: &mut ModifyList,
        tx_opt: &mut Option<TxOption>,
        collection: &mut GuardCollection,
    ) -> Option<PageGuardEP> {
        let mut guard = page_guard;
        assert_eq!(guard.latch_type(), PLatchType::WriteLock);
        let mut mut_page = PageBTreeMut::<LEAF_PAGE>::from(&mut guard);
        let sequence = modify_list.sequence();
        let ok = mut_page.leaf_upsert_key_value(index, upsert, key, value, modify_list, tx_opt);
        if ok {
            let l_type = guard.latch_type();
            let page_id = guard.page_id();
            if sequence != modify_list.sequence() {
                let _ = self
                    .lock_page_level(page_id, PLatchType::ParentModification, collection)
                    .instrument(trace_span!("lock page, ParentModification"))
                    .await;
            }
            collection
                .unlock_page(page_id, l_type)
                .instrument(trace_span!("unlock page,", "{}", l_type))
                .await;
            None
        } else {
            Some(guard)
        }
    }

    async fn update_root_page_id(&self, opt_root_id_level: Option<(PageId, u32)>) {
        match opt_root_id_level {
            Some((id, level)) => {
                let mut root = self
                    .root_page
                    .lock()
                    .instrument(trace_span!("lock root"))
                    .await;

                if root.id != id || root.level != level {
                    root.id = id;
                    root.level = level
                }
            }
            None => {}
        }
    }
    async fn get_root(&self) -> (PageId, u32) {
        let root = self
            .root_page
            .lock()
            .instrument(trace_span!("lock root"))
            .await;
        (root.id, root.level)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn search_range_gut<F, K>(
        &self,

        range: &RangeBounds<K>,
        oid: OID,
        handler: &F,
        out: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let mut guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        trace!("search range task id {}", guards.task_id());

        let range_or_key = RangeOrKey::<'_, '_, K, EmptySlice>::Range((range, oid));

        let (mut guard, _) = self
            .path_down_with_cursor(None, PLatchType::ReadLock, &range_or_key, &mut guards, out)
            .instrument(trace_span!("search path_down"))
            .await?;

        if !is_leaf_page(&guard) {
            panic!("cannot be a non leaf page");
        }
        let page = PageBTree::<LEAF_PAGE>::from(&guard);

        let index = match range.low() {
            Bound::Unbounded => 0,
            Bound::Included(k) | Bound::Excluded(k) => {
                let lb_result = page.lower_bound::<K, C>(k, &self.key_cmp);
                match lb_result {
                    LBResult::Found(index, _) => index,
                    LBResult::NotFound => {
                        guards.release_all().await;
                        return Err(ET::PageUpdateErrorNoSuchKey);
                    }
                }
            }
        };

        let ret = self
            .search_in_leaf_page_list(index, &range_or_key, &mut guard, handler, &mut guards, out)
            .await;
        guards.release_all().await;

        ret
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn search_gut<K, F>(
        &self,
        key: &K,
        handler: &F,
        tx_option: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        K: Slice,
        F: HandleReadKV,
    {
        let mut guards = GuardCollection::new(self.file_id).enable_debug_latch(self.enable_debug);
        debug!("search key task {}", guards.task_id());
        let range_or_key = RangeOrKey::<'_, '_, EmptySlice, K>::Key(key);

        let (mut guard, _stack) = self
            .path_down_with_cursor::<EmptySlice, K>(
                None,
                PLatchType::ReadLock,
                &range_or_key,
                &mut guards,
                tx_option,
            )
            .instrument(trace_span!("path_down"))
            .await?;
        let (index, find) = if is_leaf_page(&guard) {
            let page = PageBTree::<LEAF_PAGE>::from(&guard);
            let lb_result = page.lower_bound::<K, C>(key, &self.key_cmp);
            match lb_result {
                LBResult::Found(index, is_equal) => (index, is_equal),
                LBResult::NotFound => {
                    error!(
                        "cannot find key:{}",
                        self.key2json.to_json(key.as_slice()).unwrap().to_string()
                    );
                    error!(
                        "in page {}",
                        page.to_json(&self.key2json, &self.value2json)
                            .unwrap()
                            .to_string()
                    );
                    panic!("TODO")
                }
            }
        } else {
            let page = PageBTree::<NON_LEAF_PAGE>::from(&guard);
            let lb_result = page.lower_bound::<K, C>(key, &self.key_cmp);
            match lb_result {
                LBResult::Found(index, is_equal) => (index, is_equal),
                LBResult::NotFound => {
                    panic!("TODO")
                }
            }
        };

        if find {
            let ret = self
                .search_in_leaf_page_list(
                    index,
                    &range_or_key,
                    &mut guard,
                    handler,
                    &mut guards,
                    tx_option,
                )
                .instrument(trace_span!("search_in_leaf_page_list"))
                .await;

            guards
                .release_all()
                .instrument(trace_span!("Collection::release_all"))
                .await;
            ret
        } else {
            guard.unlock().await;
            Ok(0)
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn search_in_leaf_page_list<F, K1, K2>(
        &self,
        index: u32,                                // started page id
        range_or_key: &RangeOrKey<'_, '_, K1, K2>, // search key or a range ...
        guard: &mut PageGuardEP,                   // searched page
        async_handler: &F,                         // async callback function
        guards: &mut GuardCollection,
        out: &mut Option<TxOption>,
    ) -> Result<u32>
    where
        F: HandleReadKV,
        K1: Slice,
        K2: Slice,
    {
        let (mut count, mut opt_next) = self
            .search_in_leaf_page(index, range_or_key, guard, async_handler, out)
            .instrument(trace_span!("search_in_leaf_page"))
            .await?;
        let mut opt_prev = None;
        while opt_next.is_some() {
            let page_id = unsafe { opt_next.unwrap_unchecked() };

            let mut g = self
                .lock_page(page_id, PLatchType::ReadLock, guards)
                .instrument(trace_span!("search in leaf, lock page"))
                .await?;
            let (n, next) = self
                .search_in_leaf_page(0, range_or_key, &mut g, async_handler, out)
                .instrument(trace_span!("search_in_leaf_page"))
                .await?;
            count += n;

            match opt_prev {
                Some(prev) => {
                    guards
                        .unlock_page(prev, PLatchType::ReadLock)
                        .instrument(trace_span!("unlock_page"))
                        .await;
                }
                None => {}
            }
            opt_prev = Some(page_id);
            opt_next = next;
        }
        Ok(count)
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn search_in_leaf_page<F, K1, K2>(
        &self,
        start_index: u32,
        range_or_key: &RangeOrKey<'_, '_, K1, K2>,
        guard: &mut PageGuardEP,
        async_handler: &F,
        opt_tx: &mut Option<TxOption>,
    ) -> Result<(u32, Option<PageId>)>
    where
        F: HandleReadKV,
        K1: Slice,
        K2: Slice,
    {
        let mut n = 0;
        let (count, right_id) = {
            let page = PageBTree::<LEAF_PAGE>::from(guard);
            (page.get_count(), page.get_right_id())
        };

        let mut index = start_index;
        while index < count {
            match range_or_key {
                RangeOrKey::Key(key) => {
                    {
                        let page_seq_no = { PageHdr::from(guard).get_seq_no() };
                        guard
                            .test_contain_key::<K2, BtPredCtx<C>, BtPred, BtPredTable>(
                                *key,
                                &self.bt_pred_ctx,
                                page_seq_no,
                                opt_tx,
                            )
                            .await?;
                    }
                    {
                        let page = PageBTree::<LEAF_PAGE>::from(guard);

                        let k = page.get_key(index);
                        let v = page.get_value(index);
                        let slot = page.get_slot(index);
                        let ord =
                            bt_key_cmp_empty_as_max(&self.key_cmp, key.as_slice(), k.as_slice());
                        if ord.is_eq() {
                            async_handler.on_read(k, v).await?;
                            n += 1;

                            if slot.is_write_locked() {
                                match opt_tx {
                                    Some(opt) => {
                                        let conflict =
                                            opt.add_conflict_key(Vec::from(k.as_slice()));
                                        if conflict {
                                            let cursor = opt.mutable_cursor();
                                            let page_id = page.get_id();
                                            let seq_no = page.get_seq_no();
                                            cursor.set_page_id(page_id);
                                            cursor.set_slot_no(index);
                                            cursor.set_seq(seq_no);
                                            return Err(ET::TxConflict);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        } else {
                            return Ok((n, None));
                        }
                    }
                    match opt_tx {
                        None => {}
                        Some(opt) => {
                            opt.add_set_key(Vec::from(key.as_slice()), guard.page_id());
                            let mut page = PageBTreeMut::<LEAF_PAGE>::from(guard);
                            let mut slot = page.get_slot_mut(index);
                            slot.set_read_flag();
                        }
                    }
                }
                RangeOrKey::Range((r, _)) => {
                    let page = PageBTree::<LEAF_PAGE>::from(guard);
                    assert_eq!(page.get_count(), count);
                    let k = page.get_key(index);
                    if !k.is_empty() {
                        // not inf value
                        let v = page.get_value(index);
                        let slot = page.get_slot(index);
                        let range_contain = r.contain(k, &self.key_cmp);
                        match range_contain {
                            RangeContain::NotContainLeft => {}
                            RangeContain::NotContainRight => {
                                return Ok((n, None));
                            }
                            RangeContain::Contain => {
                                async_handler.on_read(k, v).await?;
                                n += 1;
                            }
                        }
                        match opt_tx {
                            None => {}
                            Some(opt) => {
                                if slot.is_write_locked() {
                                    let conflict = opt.add_conflict_key(Vec::from(k.as_slice()));
                                    if conflict {
                                        let page_id = page.get_id();
                                        let seq_no = page.get_seq_no();
                                        opt.mutable_cursor().set_page_id(page_id);
                                        opt.mutable_cursor().set_slot_no(index);
                                        opt.mutable_cursor().set_seq(seq_no);
                                        return Err(ET::TxConflict);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            index += 1;
        }
        let opt_right = if right_id != INVALID_PAGE_ID {
            Some(right_id)
        } else {
            None
        };
        Ok((n, opt_right))
    }

    fn split_page<'a, const LEAF: bool>(
        &self,
        split_position: u32,
        left_page: &mut PageBTreeMut<'a, LEAF>,
        right_page: &mut PageBTreeMut<'a, LEAF>,
        modify_list: &mut ModifyList,
    ) -> Result<()> {
        trace!(
            "before split, {}",
            left_page
                .to_json(&self.key2json, &self.value2json)
                .unwrap()
                .to_string()
        );
        left_page.split_page(right_page, split_position, modify_list);

        // assign a's right link

        trace!(
            "after split left, {}",
            left_page
                .to_json(&self.key2json, &self.value2json)
                .unwrap()
                .to_string()
        );
        trace!(
            "after split right, {}",
            right_page
                .to_json(&self.key2json, &self.value2json)
                .unwrap()
                .to_string()
        );

        Ok(())
    }

    async fn path_down_with_cursor<K1, K2>(
        &self,
        _opt_cursor: Option<Cursor>, // started from cursor fixme
        mode: PLatchType,
        range_or_key: &RangeOrKey<'_, '_, K1, K2>,
        guard_set: &mut GuardCollection,
        out: &mut Option<TxOption>,
    ) -> Result<(PageGuardEP, VecDeque<(PageId, u32)>)>
    where
        K1: Slice,
        K2: Slice,
    {
        let (root_id, _) = self.get_root().await;
        let mut stack = VecDeque::new();
        let r = self
            .path_down(
                &mut stack,
                root_id,
                None, // TODO opt cursor
                mode,
                range_or_key,
                guard_set,
                out,
            )
            .instrument(trace_span!("path_down"))
            .await;
        return match r {
            Err(e) => {
                if e == ET::ErrorCursor {
                    // retry with key
                    debug!("error cursor, and retry");
                    stack.clear();
                    guard_set
                        .release_all()
                        .instrument(trace_span!("Collection::release_all"))
                        .await;
                    let g = self
                        .path_down(
                            &mut stack,
                            root_id,
                            None,
                            mode,
                            range_or_key,
                            guard_set,
                            out,
                        )
                        .instrument(trace_span!("path_down"))
                        .await?;
                    Ok((g, stack))
                } else {
                    Err(e.clone())
                }
            }
            Ok(g) => Ok((g, stack)),
        };
    }

    async fn path_down<K1, K2>(
        &self,
        stack: &mut VecDeque<(PageId, u32)>,
        root_id: PageId,
        opt_cursor: Option<Cursor>, // started from cursor
        mode: PLatchType,
        range_or_key: &RangeOrKey<'_, '_, K1, K2>,
        guard_set: &mut GuardCollection,
        out: &mut Option<TxOption>,
    ) -> Result<PageGuardEP>
    where
        K1: Slice,
        K2: Slice,
    {
        debug!(
            "path down search, task id {}, root_id {} {:?}",
            guard_set.task_id(),
            root_id,
            opt_cursor
        );
        let start_id = match &opt_cursor {
            None => root_id,
            Some(c) => c.page_id(),
        };
        let mut id = start_id;
        let result;
        let mut parent: Option<PageGuardEP> = None;

        loop {
            if !stack.is_empty() {
                assert_ne!(stack.back().unwrap().0, id);
            }

            guard_set.reset_current();
            let first = {
                if id != root_id {
                    self.intent_access_and_upgrade(id, PLatchType::ReadLock, parent, guard_set)
                        .await?
                } else {
                    self.lock_page_level(id, PLatchType::ReadLock, guard_set)
                        .instrument(trace_span!("path down -> lock page"))
                        .await?
                }
            };
            let level = {
                let hdr = PageHdrBt::from(&first);
                hdr.get_page_level()
            };
            stack.push_back((id, level));

            if opt_cursor.is_some() && id == start_id {
                let page = PageHdr::from(&first);
                let seq_no = unsafe { *(&opt_cursor.as_ref().unwrap_unchecked().seq_no()) };
                if page.get_seq_no() != seq_no {
                    // the cursor is changed ...
                    // later, we would search from start ...
                    return Err(ET::ErrorCursor);
                }
            }
            let is_leaf = is_leaf_page(&first);
            if is_leaf {
                let guard = self
                    .path_right::<true, _, _>(id, Some(first), None, range_or_key, guard_set, out)
                    .instrument(trace_span!("leaf path right"))
                    .await?;
                let ret_guard = if mode.ne(&guard.latch_type()) {
                    assert_eq!(mode, PLatchType::WriteLock);
                    guard_set
                        .upgrade(guard.page_id(), guard.latch_type(), mode)
                        .await?
                } else {
                    guard
                };
                result = Ok(ret_guard);
                break;
            } else {
                let guard = self
                    .path_right::<false, _, _>(id, Some(first), None, range_or_key, guard_set, out)
                    .instrument(trace_span!("non leaf path right"))
                    .await?;

                let page = PageBTree::<NON_LEAF_PAGE>::from(&guard);
                let opt_key = match range_or_key {
                    RangeOrKey::Range((r, _)) => match r.low() {
                        Bound::Excluded(k) => Some(k.as_slice()),
                        Bound::Included(k) => Some(k.as_slice()),
                        Bound::Unbounded => None,
                    },
                    RangeOrKey::Key(k) => Some(k.as_slice()),
                };
                let index = match opt_key {
                    Some(key) => {
                        let lb_result =
                            page.lower_bound::<_, C>(&SliceRef::new(key), &self.key_cmp);
                        let (index, _) = match lb_result {
                            LBResult::Found(index, is_equal) => (index, is_equal),
                            LBResult::NotFound => {
                                error!(
                                    "load page error: key: {}",
                                    self.key2json.to_json(key).unwrap().to_string()
                                );
                                error!(
                                    "load page error: page:{}",
                                    page.to_json(&self.key2json, &self.key2json)
                                        .unwrap()
                                        .to_string()
                                );
                                panic!("we must found, the searching key must be lower than the high key")
                            }
                        };
                        index
                    }
                    None => {
                        if page.get_count() == 0 {
                            panic!("error, cannot a empty page")
                        }
                        0
                    }
                };

                let slot = page.get_slot(index);
                let new_id = slot.get_page_id();
                if new_id == id {
                    error!(
                        "page : {}",
                        page.to_json(&self.key2json, &self.value2json)
                            .unwrap()
                            .to_string()
                    );
                    panic!("error")
                }
                id = new_id;
                if id == INVALID_PAGE_ID {
                    panic!("TODO, cannot happen")
                }

                assert_ne!(id, INVALID_PAGE_ID);
                parent = Some(guard);
            }
            guard_set.reset_current();
        }
        // debug!("path down search done");
        return result;
    }

    #[cfg_attr(debug_assertions, inline(never))]
    async fn path_right<const LEAF: bool, K1, K2>(
        &self,
        page_id: PageId,
        first: Option<PageGuardEP>,
        parent: Option<PageGuardEP>,
        range_or_key: &RangeOrKey<'_, '_, K1, K2>,
        guard_set: &mut GuardCollection,
        out: &mut Option<TxOption>,
    ) -> Result<PageGuardEP>
    where
        K1: Slice,
        K2: Slice,
    {
        // debug!("path right search");
        let mut examined_page_id = page_id;

        let mut opt_first = first;
        let mut opt_left: Option<PageGuardEP> = None;
        let mut opt_parent = parent;
        loop {
            let guard = match opt_first {
                Some(g) => g,
                None => {
                    let g = self
                        .intent_access_and_upgrade(
                            examined_page_id,
                            PLatchType::ReadLock,
                            opt_left,
                            guard_set,
                        )
                        .await?;
                    g
                }
            };

            opt_first = None;

            let key = match range_or_key {
                RangeOrKey::Range((r, id)) => {
                    let is_root = true;
                    let page = PageBTree::<LEAF>::from(&guard);
                    let contain = page.contain::<_, _>(r, &self.key_cmp);
                    if contain || is_root {
                        let f = |key: &K1| -> Vec<u8> { Vec::from(key.as_slice()) };
                        let range = r.map(&f);
                        let pred = BtPred::new(range);
                        guard
                            .add_pred::<BtPredCtx<C>, BtPred, BtPredTable>(*id, pred, out)
                            .await;
                        match &mut opt_parent {
                            Some(parent) => {
                                parent
                                    .remove_pred::<BtPredCtx<C>, BtPred, BtPredTable>(id)
                                    .await;
                            }
                            None => {
                                // there is possible such cases when node splitting, the predicate
                                // was added to child node but cannot not be removed from parent node,
                                // it is ok for correctness, but maybe not very efficient for later
                                // search operations.
                            }
                        }
                    }
                    match r.low() {
                        Bound::Included(k) => k.as_slice(),
                        Bound::Excluded(k) => k.as_slice(),
                        Bound::Unbounded => {
                            return Ok(guard);
                        }
                    }
                }
                RangeOrKey::Key(k) => k.as_slice(),
            };

            opt_parent = None;

            let page = PageBTree::<LEAF>::from(&guard);
            if page.get_count() == 0 {
                examined_page_id = page.get_right_id();
                if examined_page_id == INVALID_PAGE_ID {
                    panic!("error, empty page to be removed, and there is no next key")
                } else {
                    opt_left = Some(guard);
                    continue;
                }
            }
            let last_key = match page.get_last_key() {
                Some(k) => k,
                None => {
                    panic!("error, at least has one slot")
                }
            };

            let ord = self.compare_key(last_key, key);
            match ord {
                Ordering::Less => {
                    // last key < key, must find next page
                    // we followed the wrong link and we need to move right node
                    examined_page_id = page.get_right_id();
                    if examined_page_id == INVALID_PAGE_ID {
                        return Ok(guard);
                    } else {
                        opt_left = Some(guard);
                    }
                }
                _ => {
                    return Ok(guard);
                }
            }
        }
    }

    fn compare_key(&self, k1: &[u8], k2: &[u8]) -> Ordering {
        bt_key_cmp_empty_as_max(&self.key_cmp, k1, k2)
    }

    fn json_error(r: json::Result<()>) -> Result<()> {
        match r {
            Err(e) => Err(ET::JSONError(e.to_string())),
            Ok(()) => Ok(()),
        }
    }

    pub async fn check_invariant(&self) -> Result<()> {
        let root_id = {
            let root = self.root_page.lock().await;
            root.id
        };
        let mut guard_set = GuardCollection::new(self.file_id);
        self.gut_check_invariant(root_id, &mut guard_set).await?;
        guard_set.release_all().await;
        Ok(())
    }

    #[async_recursion(? Send)]
    async fn check_invariant_leaf_or_not<const IS_LEAF: bool>(
        &self,
        guard: &mut PageGuardEP,
        guard_set: &mut GuardCollection,
    ) -> Result<()> {
        let page = PageBTree::<IS_LEAF>::from(guard);
        let right_id = page.get_right_id();
        if right_id != INVALID_PAGE_ID {
            let right_guard = self
                .lock_page(right_id, PLatchType::ReadLock, guard_set)
                .instrument(trace_span!("check_invariant_leaf_or_not -> lock_page"))
                .await?;
            let right_page = PageBTree::<IS_LEAF>::from(&right_guard);
            page.check_left_lt_right(&right_page, &self.key_cmp, &self.key2json, &self.value2json);
        }

        if !IS_LEAF {
            let n = page.get_count();
            for i in 0..n {
                let slot = page.get_slot(i);
                let child_id = slot.get_page_id();
                if child_id == INVALID_PAGE_ID {
                    continue;
                }
                let child_guard = self
                    .lock_page(child_id, PLatchType::ReadLock, guard_set)
                    .instrument(trace_span!("check -> lock_page"))
                    .await?;
                if !IS_LEAF {
                    if is_leaf_page(&child_guard) {
                        let child_page = PageBTree::<LEAF_PAGE>::from(&child_guard);
                        child_page.check_child_le_parent(&page, &self.key_cmp);
                    } else {
                        let child_page = PageBTree::<NON_LEAF_PAGE>::from(&child_guard);
                        child_page.check_child_le_parent(&page, &self.key_cmp);
                    }
                }
                self.gut_check_invariant(child_id, guard_set).await?;
            }
        }
        page.check_invariant(&self.key_cmp);
        page.check_high_key();
        Ok(())
    }

    #[async_recursion(? Send)]
    async fn gut_check_invariant(
        &self,
        page_id: PageId,
        collection: &mut GuardCollection,
    ) -> Result<()> {
        let mut guard = self
            .lock_page(page_id, PLatchType::ReadLock, collection)
            .instrument(trace_span!("gut_check_invariant -> lock_page"))
            .await?;
        if is_leaf_page(&guard) {
            self.check_invariant_leaf_or_not::<LEAF_PAGE>(&mut guard, collection)
                .await?;
        } else {
            self.check_invariant_leaf_or_not::<NON_LEAF_PAGE>(&mut guard, collection)
                .await?;
        }
        Ok(())
    }

    #[async_recursion(? Send)]
    async fn gut_to_json(
        &self,
        page_id: PageId,
        pool: &BufPool,
        guard_set: &mut GuardCollection,
    ) -> Result<JsonValue> {
        let mut guard = self
            .lock_page(page_id, PLatchType::ReadLock, guard_set)
            .instrument(trace_span!("to_json -> lock_page"))
            .await?;

        let (is_leaf, n) = {
            let page = PageHdrBt::from(&guard);
            (page.is_leaf(), page.get_count())
        };

        let mut json = object! {};
        let mut array = array![];
        for i in 0..n {
            let mut j_kv = object! {};
            if !is_leaf {
                let (key_json, child_id) = {
                    let p = PageBTree::<NON_LEAF_PAGE>::from(&guard);
                    let key = p.get_key(i);
                    let slot = p.get_slot(i);
                    let child_id = slot.get_page_id();
                    let key_json = if !key.is_empty() {
                        let key_json = self.key2json.to_json(key)?;
                        key_json
                    } else {
                        JsonValue::Null
                    };
                    (key_json, child_id)
                };
                assert_ne!(child_id, INVALID_PAGE_ID);
                let subtree_json = self.gut_to_json(child_id, pool, guard_set).await?;
                let r = j_kv.insert("key", key_json);
                Self::json_error(r)?;
                let r = j_kv.insert("id", child_id);
                Self::json_error(r)?;
                let r = j_kv.insert("ptr", subtree_json);
                Self::json_error(r)?;
            } else {
                let (key_json, value_json) = {
                    let p = PageBTree::<LEAF_PAGE>::from(&mut guard);
                    let key = p.get_key(i);
                    if !key.is_empty() {
                        let key_json = self.key2json.to_json(key)?;
                        let value = p.get_value(i);
                        let value_json = self.value2json.to_json(value)?;
                        (key_json, value_json)
                    } else {
                        (JsonValue::Null, JsonValue::Null)
                    }
                };
                let r = j_kv.insert("key", key_json);
                Self::json_error(r)?;
                let r = j_kv.insert("value", value_json);
                Self::json_error(r)?;
            }
            let r = array.push(j_kv);
            Self::json_error(r)?;
        }

        let meta_json = if !is_leaf {
            let p = PageBTree::<NON_LEAF_PAGE>::from(&guard);
            p.page_meta_to_json()?
        } else {
            let p = PageBTree::<LEAF_PAGE>::from(&guard);
            p.page_meta_to_json()?
        };
        let _ = json.insert("meta", meta_json);
        let _ = json.insert("array", array);
        Ok(json)
    }

    // depth first traversal

    pub async fn tree_traversal<H: HandlePage + 'static>(&self, handle: &H) -> Result<()> {
        let root_id = {
            let root = self.root_page.lock().await;
            root.id
        };
        let mut collection = GuardCollection::new(self.file_id);
        let r = self
            .tree_recursion(None, root_id, &mut collection, handle)
            .await;
        let ret = match r {
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        };
        collection.release_all().await;
        ret
    }

    #[async_recursion( ?Send)]
    async fn tree_recursion<H: HandlePage + 'static>(
        &self,
        parent: Option<PageId>,
        page_id: PageId,
        collection: &mut GuardCollection,
        handle: &H,
    ) -> Result<()> {
        let guard = self
            .lock_page(page_id, PLatchType::None, collection)
            .instrument(trace_span!("tree_recursion -> lock_page"))
            .await?;
        if is_leaf_page(&guard) {
            self.tree_recursion_leaf(parent, guard, handle).await?;
        } else {
            self.tree_recursion_non_leaf(parent, guard, collection, handle)
                .await?;
        }

        Ok(())
    }

    #[async_recursion( ?Send)]
    async fn tree_recursion_non_leaf<H: HandlePage + 'static>(
        &self,
        opt_parent: Option<PageId>,
        guard: PageGuardEP,
        guard_set: &mut GuardCollection,
        handle: &H,
    ) -> Result<()> {
        let vec = Vec::from(guard.as_slice());
        let page = PageBTree::<NON_LEAF_PAGE>::from(&vec);
        let count = page.get_count();
        let id = guard.page_id();
        for i in 0..count {
            let slot = page.get_slot(i);
            let child_id = slot.get_page_id();
            self.tree_recursion(Some(id), child_id, guard_set, handle)
                .await?;
        }
        handle.handle_non_leaf(opt_parent, guard).await?;
        Ok(())
    }

    async fn tree_recursion_leaf<H: HandlePage + 'static>(
        &self,
        opt_parent: Option<PageId>,
        guard: PageGuardEP,
        handle: &H,
    ) -> Result<()> {
        handle.handle_leaf(opt_parent, guard).await?;
        Ok(())
    }

    // try to acquire AccessIntent latch
    // mode can be ReadLock or WriteLock
    async fn intent_access_and_upgrade(
        &self,
        page_id: PageId,
        mode: PLatchType,
        opt_release: Option<PageGuardEP>,
        collection: &mut GuardCollection,
    ) -> Result<PageGuardEP> {
        collection
            .intent_access_and_upgrade::<C>(&self.pool, page_id, mode, opt_release)
            .await
    }

    async fn lock_page(
        &self,
        page_id: PageId,
        mode: PLatchType,
        collection: &mut GuardCollection,
    ) -> Result<PageGuardEP> {
        collection
            .lock_bt_page_level::<C>(&self.pool, page_id, mode, false)
            .instrument(trace_span!("lock_bt_page"))
            .await
    }

    // invoke by when updating non leaf node
    async fn lock_page_level(
        &self,
        page_id: PageId,
        mode: PLatchType,
        collection: &mut GuardCollection,
    ) -> Result<PageGuardEP> {
        collection
            .lock_bt_page_level::<C>(&self.pool, page_id, mode, true)
            .instrument(trace_span!("lock_bt_page_level"))
            .await
    }
}
