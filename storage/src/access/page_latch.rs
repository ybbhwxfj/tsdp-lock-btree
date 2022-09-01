use async_std::future::timeout;
use std::any::Any;

use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use async_std::sync::Condvar;

use atomic::Atomic;
use json::{object, JsonValue};

use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug_span, error, info, info_span, trace};
use tracing_futures::Instrument;

use adt::slice::Slice;
use common::error_type::ET;
use common::id::{TaskId, OID};
use common::result::Result;

use crate::access::access_opt::TxOption;
use crate::access::latch_type::PLatchType;
use crate::access::page_id::{PageId, PageIndex};
use crate::access::predicate::{Pred, PredCtx, PredTable};
use crate::access::unsafe_shared::UnsafeShared;

#[derive(Clone)]
pub struct PageLatch<T: Sized> {
    page_index: PageIndex,
    inner: Arc<Mutex<PLatchInner<T>>>,
}

#[derive(Clone)]
pub struct PageGuard<T: Sized> {
    locked: Arc<Atomic<bool>>,
    page_index: PageIndex,
    item: PLatchItem,

    inner: Arc<Mutex<PLatchInner<T>>>,
    // ptr pointer inner member's PLatchInner::data and have the same life cycle
    // it is safe to shared this pointer
    ptr: UnsafeShared<T>,
}

pub type PageLatchEP = PageLatch<Vec<u8>>;

pub type PGuardEP<T> = PageGuard<T>;

pub type PLatchEP<T> = PageLatch<T>;

pub type PageGuardEP = PageGuard<Vec<u8>>;

struct PLatchInner<T: Sized> {
    page_index: PageIndex,
    opt_level: Option<u32>,
    access_intent_count: u32,
    read_count: u32,
    is_write: bool,
    is_node_delete: bool,
    is_parent_modification: bool,
    queue: VecDeque<(Arc<LatchNotify>, PLatchType)>,
    predicate_table: Box<dyn Any>,
    // lock acquired ...
    lock: HashMap<TaskId, u32>,
    data: T,
}

unsafe impl<T: Sized> Send for PageLatch<T> {}

unsafe impl<T: Sized> Sync for PageLatch<T> {}

unsafe impl<T: Sized> Send for PageGuard<T> {}

unsafe impl<T: Sized> Sync for PageGuard<T> {}

unsafe impl<T: Sized> Send for PLatchInner<T> {}

unsafe impl<T: Sized> Sync for PLatchInner<T> {}

struct LatchNotify {
    id: TaskId,
    wait: async_std::sync::Mutex<bool>,
    condvar: Condvar,
}

#[derive(Clone)]
struct PLatchItem {
    l_type: PLatchType,
    notify: Arc<LatchNotify>,
}

impl Default for LatchNotify {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Debug for LatchNotify {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "task:{:?}", self.id)
    }
}

impl LatchNotify {
    fn new(id: TaskId) -> Self {
        Self {
            id,
            wait: async_std::sync::Mutex::new(false),
            condvar: Default::default(),
        }
    }

    fn id(&self) -> TaskId {
        self.id
    }

    async fn notify(&self) {
        let mut wait = self.wait.lock().await;
        *wait = false;
        self.condvar.notify_all();
    }

    async fn set_wait(&self) {
        let mut wait = self.wait.lock().await;
        *wait = true;
    }

    async fn wait_notified(&self) {
        let mut wait = self.wait.lock().await;
        while *wait {
            wait = self.condvar.wait(wait).await;
        }
    }
}

impl Eq for PLatchItem {}

impl PartialEq<Self> for PLatchItem {
    fn eq(&self, other: &Self) -> bool {
        let p1 = self.notify.as_ref() as *const LatchNotify;
        let p2 = other.notify.as_ref() as *const LatchNotify;
        std::ptr::eq(p1, p2)
    }
}

impl Hash for PLatchItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let iv = (self.notify.as_ref() as *const LatchNotify) as u64;
        iv.hash(state)
    }
}

impl PLatchItem {
    pub fn make(l_type: PLatchType, notify: Arc<LatchNotify>) -> PLatchItem {
        let _i = l_type as u32;
        PLatchItem { l_type, notify }
    }
}

impl Debug for PLatchItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //self.notify.as_ref().fmt(f)?;
        write!(f, "id: {:?}, ", self.notify.id)?;

        write!(f, "flag:")?;
        write!(f, "{:?}, ", self.l_type)?;
        Ok(())
    }
}

impl<T: Sized> PLatchInner<T> {
    pub fn new(page_index: PageIndex, t: T, predicate_table: Box<dyn Any>) -> PLatchInner<T> {
        PLatchInner {
            page_index,
            opt_level: None,
            access_intent_count: 0,
            read_count: 0,
            is_write: false,
            is_node_delete: false,
            is_parent_modification: false,
            queue: Default::default(),
            predicate_table,
            lock: Default::default(),
            data: t,
        }
    }

    pub fn set_level(&mut self, level: u32) {
        self.opt_level = Some(level);
    }

    #[allow(dead_code)]
    pub fn page_index(&self) -> PageIndex {
        self.page_index
    }

    pub fn mut_ptr(&mut self) -> UnsafeShared<T> {
        UnsafeShared::new(&mut self.data)
    }

    #[allow(dead_code)]
    pub fn to_json(&self) -> JsonValue {
        let mut queue = vec![];
        for e in &self.queue {
            queue.push(format!("{:?}", e));
        }

        let v = object! {
            "file_id" : self.page_index.file_id().to_string(),
            "page_id" : self.page_index.page_id(),
            "access_intent_count": self.access_intent_count,
            "read_count":self.read_count,
            "is_write":self.is_write,
            "is_node_delete":self.is_node_delete,
            "is_parent_modification:":self.is_parent_modification,
            "queue": queue,
        };
        v
    }

    pub fn lock(&mut self, lock_type: PLatchType, notify: Arc<LatchNotify>) -> bool {
        let task_id = notify.id();
        if self.queue.is_empty()
            || lock_type == PLatchType::None
            || (self.lock.contains_key(&task_id))
        {
            // if there we have held a WriteLock, and we want to acquire another ParentModification lock
            // we must check the compatibility ignore the wait queue to avoid deadlock occur
            let ok = self.test_and_set_lock_type(task_id, lock_type);
            if !ok {
                self.queue.push_back((notify, lock_type));
                false
            } else {
                self.insert_acquired(task_id, lock_type);
                true
            }
        } else {
            self.queue.push_back((notify, lock_type));
            false
        }
    }

    pub fn lock_upgrade(&mut self, item: &PLatchItem, lock_type: PLatchType) -> bool {
        let ok = self.remove_released(item.notify.id(), item.l_type);
        if !ok {
            panic!("no such lock");
        }
        match (item.l_type, lock_type) {
            (PLatchType::AccessIntent, PLatchType::ReadLock) => {
                self.access_intent_count -= 1;
            }
            (PLatchType::AccessIntent, PLatchType::WriteLock) => {
                self.access_intent_count -= 1;
            }
            (PLatchType::WriteLock, PLatchType::NodeDelete) => {
                self.is_write = false;
            }
            (PLatchType::WriteLock, PLatchType::ParentModification) => {
                self.is_write = false;
            }
            (PLatchType::ReadLock, PLatchType::ParentModification) => {
                self.read_count -= 1;
            }
            (PLatchType::ReadLock, PLatchType::WriteLock) => {
                self.read_count -= 1;
            }
            _ => {
                if item.l_type.eq(&lock_type) {
                    return true;
                } else {
                    panic!(
                        "error, do not allow upgrade latch from {} to {}",
                        item.l_type, lock_type
                    );
                }
            }
        }
        let id = item.notify.id();
        if self.queue.is_empty() || (self.lock.contains_key(&id)) {
            // wait queue is empty, or a parent modification lock
            let ok = self.test_and_set_lock_type(id, lock_type);
            if !ok {
                self.queue.push_back((item.notify.clone(), lock_type));
            } else {
                self.insert_acquired(id, lock_type);
            }
            ok
        } else {
            self.queue.push_back((item.notify.clone(), lock_type));
            false
        }
    }

    pub async fn inner_unlock(&mut self, latch: &PLatchItem) {
        let ok = self.remove_released(latch.notify.id(), latch.l_type);
        if !ok {
            panic!("no such lock");
        }
        self.un_lock_gut(latch.l_type).await;
    }

    fn insert_acquired(&mut self, task_id: TaskId, lock_type: PLatchType) {
        for (k, flag) in self.lock.iter() {
            if *k != task_id
                && PLatchType::has_flag(PLatchType::WriteLock, *flag)
                && lock_type == PLatchType::WriteLock
            {
                trace!("error, {:?}", self);
                trace!("error, {} {}", task_id, lock_type)
            }
        }
        let opt = self.lock.get_mut(&task_id);
        match opt {
            Some(flag) => {
                *flag = PLatchType::set_flag(lock_type, *flag);
            }
            None => {
                let _ = self.lock.insert(task_id, PLatchType::flag(lock_type));
            }
        }
    }

    fn remove_released(&mut self, task_id: TaskId, lock_type: PLatchType) -> bool {
        let opt = self.lock.get_mut(&task_id);
        match opt {
            Some(flag) => {
                if !PLatchType::has_flag(lock_type, *flag) {
                    false
                } else {
                    let f = PLatchType::clr_flag(lock_type, *flag);
                    if f != 0 {
                        *flag = f;
                    } else {
                        self.lock.remove(&task_id);
                    }
                    true
                }
            }
            None => false,
        }
    }
    fn not_locked(&self, debug: bool) -> bool {
        let ret = !self.is_write
            && !self.is_node_delete
            && !self.is_parent_modification
            && !self.access_intent_count == 0
            && !self.read_count == 0;
        if ret && debug {
            error!("{:?}", self)
        }
        ret
    }

    // used for debug
    fn task_wait_for(&mut self) -> (Vec<TaskId>, Vec<TaskId>) {
        let mut vec_hold = Vec::new();
        let mut vec_wait = Vec::new();
        for (id, _flag) in &self.lock {
            vec_hold.push(*id)
        }
        for (l, _l_type) in &self.queue {
            vec_wait.push(l.id());
        }
        (vec_wait, vec_hold)
    }

    async fn try_notify(&mut self) {
        loop {
            let op = self.queue.front();
            let (nt, l_type) = match op {
                Some((nt, lt)) => (nt.clone(), lt.clone()),
                None => {
                    return;
                }
            };
            let id = nt.id();
            let ok = self.test_and_set_lock_type(id, l_type);
            if ok {
                self.insert_acquired(id, l_type);
                let opt = self.queue.pop_front();
                match opt {
                    Some((n, lt)) => {
                        assert!(n.id() == id && lt == l_type);
                    }
                    None => {
                        panic!("error");
                    }
                }
                nt.notify().await;
            } else {
                return;
            }
        }
    }
    async fn un_lock_gut(&mut self, lock_type: PLatchType) {
        self.clr_lock_type(lock_type);
        self.try_notify().await;
    }

    fn test_and_set_lock_type(&mut self, id: TaskId, l_type: PLatchType) -> bool {
        let ok = self.test_lock_type(id, l_type);
        if ok {
            self.set_lock_type(l_type)
        }
        ok
    }

    fn contain_lock(&self, id: TaskId, l_type: PLatchType) -> bool {
        let opt_flag = self.lock.get(&id);
        return match opt_flag {
            None => false,
            Some(f) => PLatchType::has_flag(l_type, *f),
        };
    }
    fn test_lock_type(&self, id: TaskId, l_type: PLatchType) -> bool {
        match l_type {
            PLatchType::WriteLock => {
                ((!self.is_write)
                    || (self.is_write && self.contain_lock(id, PLatchType::WriteLock)))
                    && (self.read_count == 0
                        || (self.read_count == 1 && self.contain_lock(id, PLatchType::ReadLock)))
            }
            PLatchType::ReadLock => {
                (!self.is_write) || (self.is_write && self.contain_lock(id, PLatchType::WriteLock))
            }
            PLatchType::NodeDelete => {
                (!self.is_node_delete
                    || self.is_node_delete
                        && (self.is_node_delete && self.contain_lock(id, PLatchType::NodeDelete)))
                    && (self.access_intent_count == 0
                        || (self.access_intent_count == 1
                            && self.contain_lock(id, PLatchType::AccessIntent)))
            }
            PLatchType::AccessIntent => {
                (!self.is_node_delete)
                    || (self.is_node_delete && self.contain_lock(id, PLatchType::NodeDelete))
            }
            PLatchType::ParentModification => {
                (!self.is_parent_modification)
                    || (self.is_parent_modification
                        && self.contain_lock(id, PLatchType::ParentModification))
            }
            PLatchType::None => true,
        }
    }

    fn set_lock_type(&mut self, l_type: PLatchType) {
        match l_type {
            PLatchType::WriteLock => self.is_write = true,
            PLatchType::ReadLock => self.read_count += 1,
            PLatchType::NodeDelete => self.is_node_delete = true,
            PLatchType::AccessIntent => self.access_intent_count += 1,
            PLatchType::ParentModification => self.is_parent_modification = true,
            PLatchType::None => {}
        }
    }

    fn clr_lock_type(&mut self, l_type: PLatchType) {
        match l_type {
            PLatchType::WriteLock => self.is_write = false,
            PLatchType::ReadLock => self.read_count -= 1,
            PLatchType::NodeDelete => self.is_node_delete = false,
            PLatchType::AccessIntent => self.access_intent_count -= 1,
            PLatchType::ParentModification => self.is_parent_modification = false,
            PLatchType::None => {}
        }
    }
}

impl<T: Sized> PageGuard<T> {
    // non pub fn
    fn new(
        page_index: PageIndex,
        inner: Arc<Mutex<PLatchInner<T>>>,

        notify: PLatchItem,
        ptr: UnsafeShared<T>,
    ) -> Self {
        Self {
            locked: Arc::new(Atomic::new(false)),
            page_index,
            inner,
            item: notify,
            ptr,
        }
    }

    pub fn ptr(&self) -> *const T {
        self.ptr.const_ptr()
    }

    pub fn latch_type(&self) -> PLatchType {
        self.item.l_type
    }

    pub fn task_id(&self) -> TaskId {
        self.item.notify.id()
    }

    pub fn page_index(&self) -> PageIndex {
        self.page_index
    }

    pub fn page_id(&self) -> PageId {
        self.page_index.page_id()
    }

    pub fn to_json(&self) -> JsonValue {
        let v = object! {
            "page_index": self.page_index.to_string(),
        };
        v
    }
    pub async fn fmt(&self) -> Result<String> {
        let inner = self.inner.lock().await;
        let s = format!("item : {:?}, \n inner : {:?}\n", self.item, inner);
        Ok(s)
    }
    pub async fn set_level(&self, level: u32) {
        let mut inner = self.inner.lock().await;
        inner.set_level(level);
    }

    /// wait task id , hold task id
    pub async fn task_wait_for(&self) -> (Vec<TaskId>, Vec<TaskId>) {
        let mut inner = self.inner.lock().await;
        inner.task_wait_for()
    }

    pub async fn merge_pred<C: PredCtx, P: Pred<C>, S: PredTable<C, P>>(
        &mut self,
        left_guard: &Self,
    ) {
        let (mut g1, mut g2) = {
            // lock left first , and later right(self)
            let f1 = left_guard.inner.lock();
            let f2 = self.inner.lock();
            let g1 = match timeout(Duration::from_secs(60), f1).await {
                Ok(g) => g,
                Err(_) => {
                    panic!("error");
                }
            };
            let g2 = match timeout(Duration::from_secs(60), f2).await {
                Ok(g) => g,
                Err(_) => {
                    panic!("error");
                }
            };
            (g1, g2)
        };
        let opt_t1: Option<&mut S> = g1.predicate_table.downcast_mut::<S>();
        let opt_t2: Option<&mut S> = g2.predicate_table.downcast_mut::<S>();
        match (opt_t1, opt_t2) {
            (Some(t1), Some(t2)) => {
                t1.merge(t2);
            }
            _ => {
                panic!("error");
            }
        }
    }

    pub async fn remove_pred<C: PredCtx, P: Pred<C>, S: PredTable<C, P>>(&self, oid: &OID) {
        let mut guard = self.inner.lock().await;
        let opt_table: Option<&mut S> = guard.predicate_table.downcast_mut::<S>();
        match opt_table {
            Some(t) => {
                t.remove(oid);
            }
            None => {
                panic!("error");
            }
        }
    }

    pub async fn add_pred<C: PredCtx, P: Pred<C>, S: PredTable<C, P> + 'static>(
        &self,
        oid: OID,
        pred: P,
        out: &mut Option<TxOption>,
    ) {
        if out.is_none() {
            return;
        }

        let mut guard = self.inner.lock().await;
        let opt_table: Option<&mut S> = guard.predicate_table.downcast_mut::<S>();
        match opt_table {
            Some(t) => {
                t.add(oid, pred);
            }
            None => {
                panic!("error");
            }
        }
    }

    pub async fn test_contain_key<K: Slice, C: PredCtx, P: Pred<C>, S: PredTable<C, P>>(
        &self,
        k: &K,
        ctx: &C,
        page_seq_no: u32,
        out: &mut Option<TxOption>,
    ) -> Result<()> {
        let opt = match out {
            None => return Ok(()),
            Some(opt) => opt,
        };
        let mut guard = self.inner.lock().await;
        let opt_table: Option<&mut S> = guard.predicate_table.downcast_mut::<S>();
        match opt_table {
            Some(t) => {
                let (opt_conflict, opt_deleted) = t.test_contain_and_deleted(k, ctx)?;
                match opt_deleted {
                    Some(deleted_oids) => {
                        // garbage collect all deleted predicate
                        for oid in deleted_oids.iter() {
                            t.remove(oid)
                        }
                    }
                    _ => {}
                }
                match opt_conflict {
                    Some(conflict_oids) => {
                        // garbage collect all deleted predicate
                        let mut conflict = 0u64;

                        for oid in conflict_oids.iter() {
                            if opt.add_conflict_predicate(*oid) {
                                conflict += 1;
                            }
                        }
                        if conflict > 0 {
                            let cursor = opt.mutable_cursor();
                            cursor.set_page_id(self.page_id());
                            cursor.set_slot_no(0);
                            cursor.set_seq(page_seq_no);
                            return Err(ET::TxConflict);
                        }
                    }
                    _ => {}
                }
            }
            None => {
                panic!("error");
            }
        }
        Ok(())
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub async fn unlock(&self) {
        let mut guard = self.inner.lock().await;
        guard.inner_unlock(&self.item).await;
        if self.locked.load(atomic::Ordering::SeqCst) {
            // set unlock flag
            self.locked.store(false, atomic::Ordering::SeqCst);
            trace!("latch unlock {:?}, {:?}", self.item, guard);
        }
    }
    pub async fn lock(&self, lt: PLatchType) {
        let lock_ok = {
            let mut guard = self.inner.lock().await;
            let ok = guard.lock(lt, self.item.notify.clone());
            trace!("latch lock is_ok  {:?}, {:?} {:?}", ok, self.item, guard);
            ok
        };
        if !lock_ok {
            self.wait_notify()
                .instrument(debug_span!("latch_upgrade", "{}", lt.to_string()))
                .await;
        }
    }

    pub async fn wait_notify(&self) {
        loop {
            let f = self.item.notify.wait_notified();
            let r = timeout(Duration::from_secs(30000), f).await;
            match r {
                Ok(_) => {
                    break;
                }
                Err(_e) => {
                    let inner = self.inner.lock().await;
                    error!("page latch timeout, {:?} ", self.item);
                    error!("page latch timeout,  {:?}", inner)
                }
            };
        }
    }

    pub async fn upgrade(&self, lt: PLatchType) -> Self {
        let (lock_ok, notify, ptr) = {
            let mut guard = self.inner.lock().await;
            let ptr = guard.mut_ptr();

            let ok = guard.lock_upgrade(&self.item, lt);

            trace!(
                "latch upgrade is_ok  {:?}, {:?} {:?}",
                ok,
                PLatchItem::make(lt, self.item.notify.clone()),
                guard
            );
            if !ok {
                // cannot acquire the latch, must wait conflict latch release
                self.item.notify.set_wait().await;
            }
            guard.try_notify().await;
            (ok, self.item.notify.clone(), ptr)
        };

        if !lock_ok {
            self.wait_notify()
                .instrument(debug_span!("latch_upgrade", "{}", lt.to_string()))
                .await;
        }
        if self.locked.load(atomic::Ordering::SeqCst) {
            // set unlock flag
            self.locked.store(false, atomic::Ordering::SeqCst);
        }
        Self::new(
            self.page_index,
            self.inner.clone(),
            PLatchItem::make(lt, notify),
            ptr,
        )
    }
}

impl<T: Sized> Deref for PageGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &(*self.ptr.const_ptr()) }
    }
}

impl<T: Sized> DerefMut for PageGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut (*self.ptr.mut_ptr()) }
    }
}

impl<T: Sized> Drop for PageGuard<T> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.locked) == 1 {
            // the last reference
            let locked = self.locked.load(atomic::Ordering::SeqCst);
            if locked {
                panic!("latch leak")
            }
        }
    }
}

impl<T: Sized> PageLatch<T> {
    pub fn new(page_index: PageIndex, t: T, table: Box<dyn Any>) -> Self
    where
        T: Sized,
    {
        PageLatch {
            page_index,
            inner: Arc::new(Mutex::new(PLatchInner::new(page_index, t, table))),
        }
    }

    pub async fn to_debug_string(&self) -> String {
        let inner = self.inner.lock().await;
        format!("{:?}", inner)
    }

    pub async fn lock(&self, l_type: PLatchType, id: TaskId) -> PageGuard<T> {
        let (ok, p_guard) = {
            let mut inner: MutexGuard<PLatchInner<T>> = self
                .inner
                .lock()
                .instrument(debug_span!("latch lock"))
                .await;
            let notify = Arc::new(LatchNotify::new(id));
            let ok = inner.lock(l_type, notify.clone());
            trace!(
                "latch lock {} {} {:?}, {:?}",
                id,
                l_type,
                self.page_index,
                inner
            );

            if !ok {
                notify.set_wait().await;
            }
            let ptr = inner.mut_ptr();
            let p_guard = PageGuard::new(
                self.page_index,
                self.inner.clone(),
                PLatchItem::make(l_type, notify),
                ptr,
            );

            (ok, p_guard)
        };

        if !ok {
            p_guard
                .wait_notify()
                .instrument(debug_span!(
                    "latch, wait notified",
                    "{}",
                    l_type.to_string()
                ))
                .await;
            p_guard.locked.store(true, atomic::Ordering::SeqCst);
        } else {
            p_guard.locked.store(true, atomic::Ordering::SeqCst);
        }
        p_guard
    }

    pub fn page_index(&self) -> PageIndex {
        self.page_index
    }

    pub async fn not_locked(&self, debug: bool) -> bool {
        let inner = self.inner.lock().await;
        inner.not_locked(debug)
    }
}

pub fn guard_add(guards: &mut HashMap<PageId, PageGuardEP>, page_id: PageId, guard: PageGuardEP) {
    guards.insert(page_id, guard);
}

pub async fn guard_release_all(guards: &mut HashMap<PageId, PageGuardEP>) {
    for (_, g) in guards.iter_mut() {
        g.unlock()
            .instrument(info_span!("guard_release_all -> Guard::unlock"))
            .await;
    }
    guards.clear();
}

pub async fn guard_release_one(guards: &mut HashMap<PageId, PageGuardEP>, page_id: PageId) {
    let opt_value = guards.remove(&page_id);
    match opt_value {
        Some(g) => {
            g.unlock()
                .instrument(info_span!("guard_release_one -> Guard::unlock"))
                .await;
        }
        None => {}
    }
}

impl<T: Sized> Debug for PLatchInner<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "index:[{}, {}], ",
            self.page_index.file_id(),
            self.page_index.page_id()
        )?;
        write!(f, "level: {:?}, ", self.opt_level)?;
        write!(f, "write: {}, ", self.is_write)?;
        write!(f, "parent: {}, ", self.is_parent_modification)?;
        write!(f, "delete: {}, ", self.is_node_delete)?;
        write!(f, "read: {}, ", self.read_count)?;
        write!(f, "access: {}, ", self.access_intent_count)?;
        write!(f, "queue: {:?}, ", self.queue)?;
        write!(f, "lock: {:?} ", self.lock)?;
        Ok(())
    }
}
