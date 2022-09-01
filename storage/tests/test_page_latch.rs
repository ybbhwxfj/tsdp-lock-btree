#![feature(bound_map)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_std::sync::Mutex;
use async_std::task;

use common::id::TaskId;
use rand::{thread_rng, Rng};
use tokio::runtime::Builder;
use tracing::trace;
use tracing::{trace_span, Instrument};

use common::result::Result;
use storage::access::latch_type::PLatchType;
use storage::access::page_id::PageIndex;
use storage::access::page_latch::{PageGuard, PageLatch};
use storage::access::predicate::{EmptyPredTable, PredTableBase};

use crate::test_storage::lock_test::{TLContext, TLOpParam, TLOps};

pub mod test_storage;

type PLOperation = TLOpParam<(), PLatchType, ()>;

#[derive(Clone)]
struct PTestRoutine {
    id: u32,
    guards: Arc<Mutex<HashMap<TaskId, PageGuard<i32>>>>,
}

#[derive(Clone)]
pub struct PTestContext {
    map: HashMap<u32, PTestRoutine>,
    latch: Arc<PageLatch<i32>>,
    test_lock_ctx: Arc<TLContext<u32, (), PLatchType, ()>>,
}

unsafe impl Send for PTestContext {}

impl PTestRoutine {
    pub fn new(id: u32) -> Self {
        PTestRoutine {
            id,
            guards: Arc::new(Default::default()),
        }
    }

    pub async fn lock(
        &self,
        id: u32,
        latch: Arc<PageLatch<i32>>,
        l_type: PLatchType,
        op: PLOperation,
        ctx: &TLContext<u32, (), PLatchType, ()>,
    ) {
        let task_id = id as TaskId;
        trace!("routine {} lock", self.id);
        let opt_guard = {
            let mut map = self.guards.lock().await;
            map.remove(&task_id)
        };
        match opt_guard {
            Some(g) => {
                let upgrade_guard = g
                    .upgrade(l_type)
                    .instrument(trace_span!("lock upgrade", "{}", l_type.to_string()))
                    .await;
                let mut map = self.guards.lock().await;
                map.insert(task_id, upgrade_guard);
            }
            None => {
                let guard = latch
                    .lock(l_type.clone(), id as TaskId)
                    .instrument(trace_span!("lock", "{}", l_type.to_string()))
                    .await;

                let mut map = self.guards.lock().await;
                map.insert(task_id, guard);
            }
        };

        ctx.append_history(id, op).await;
    }

    pub async fn unlock(&self, id: u32, op: PLOperation, ctx: &TLContext<u32, (), PLatchType, ()>) {
        trace!("routine {} unlock", self.id);
        let task_id = id as TaskId;
        let opt_guard = {
            let mut map = self.guards.lock().await;
            map.remove(&task_id)
        };
        let guard = match opt_guard {
            Some(g) => g,
            None => {
                panic!("TODO");
            }
        };

        guard.unlock().await;
        ctx.append_history(id, op).await;
    }
}

impl PTestContext {
    fn new(ops: TLOps<u32, (), PLatchType, ()>) -> Self {
        let mut ctx = PTestContext {
            map: HashMap::new(),
            latch: Arc::new(PageLatch::new(
                PageIndex::new(0, 0),
                0,
                Box::new(EmptyPredTable::default()),
            )),
            test_lock_ctx: Arc::new(TLContext::new(ops)),
        };

        for (id, _) in ctx.test_lock_ctx.id_2_lock_ops_map() {
            if !ctx.map.contains_key(id) {
                let routine = PTestRoutine::new(id.clone());
                ctx.map.insert(id.clone(), routine);
            }
        }
        ctx
    }
}

pub async fn run_routine(ctx: PTestContext, ops: TLOps<u32, (), PLatchType, ()>) -> Result<()> {
    for (id, op) in ops.iter() {
        let routine = match ctx.map.get(id) {
            Some(l) => l.clone(),
            None => {
                panic!("error")
            }
        };
        match op {
            PLOperation::Barrier => {
                ctx.test_lock_ctx
                    .wait_barrier()
                    .instrument(trace_span!("wait barrier", id))
                    .await;
                continue;
            }
            PLOperation::Sleep => {
                // let others routine come first
                let mut rnd = thread_rng();
                let millis = rnd.gen_range(0..100u64);
                task::sleep(Duration::from_millis(millis)).await
            }
            PLOperation::Lock(_, _, lt) => {
                routine
                    .lock(*id, ctx.latch.clone(), *lt, op.clone(), &ctx.test_lock_ctx)
                    .instrument(trace_span!("lock", id, "{}", lt.to_string()))
                    .await;
            }
            PLOperation::Unlock(_, _, ()) => {
                routine
                    .unlock(*id, op.clone(), &ctx.test_lock_ctx)
                    .instrument(trace_span!("unlock", id))
                    .await;
            }
            _ => {
                panic!("TODO");
            }
        }
    }
    Ok(())
}

//  vec(id, sequence, latch type)
fn test_latch_operations(ops: TLOps<u32, (), PLatchType, ()>) {
    let ctx = PTestContext::new(ops);
    let id2ops = ctx.test_lock_ctx.id_2_lock_ops_map().clone();
    let thread = std::thread::spawn(move || {
        let local = tokio::task::LocalSet::new();

        for (_id, ops) in id2ops {
            let ctx_c = ctx.clone();
            local.spawn_local(async move { run_routine(ctx_c, ops).await });
        }

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(local);
        runtime.block_on(ctx.test_lock_ctx.check_history());
    });

    let _ = thread.join();
}

fn test_non_conflict(l1: PLatchType, l2: PLatchType) {
    let ops = vec![
        (1, PLOperation::Lock(1, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(2, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Unlock(3, (), ())),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Unlock(4, (), ())),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Lock(5, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(6, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Unlock(7, (), ())),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Unlock(8, (), ())),
    ];
    // for non conflict operations, non-concurrent executing by input order
    test_latch_operations(ops);
}

fn test_conflict(l1: PLatchType, l2: PLatchType) {
    let ops = vec![
        (1, PLOperation::Lock(1, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(3, (), l2)),
        (2, PLOperation::Unlock(4, (), ())),
        (1, PLOperation::Sleep),
        (1, PLOperation::Unlock(2, (), ())),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(5, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Lock(7, (), l1)),
        (1, PLOperation::Unlock(8, (), ())),
        (2, PLOperation::Sleep),
        (2, PLOperation::Unlock(6, (), ())),
    ];
    test_latch_operations(ops);
}

// l1 l2 conflict
// l2 l1_upgrade conflict
fn test_upgrade_c_c(l1: PLatchType, l1_upgrade: PLatchType, l2: PLatchType) {
    let ops = vec![
        (1, PLOperation::Lock(1, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(4, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Lock(2, (), l1_upgrade)),
        (2, PLOperation::Unlock(5, (), ())),
        (1, PLOperation::Unlock(3, (), ())),
    ];
    test_latch_operations(ops);
}

// l1 l2 non conflict
// l2 l1_upgrade conflict
fn test_upgrade_nc_c(l1: PLatchType, l1_upgrade: PLatchType, l2: PLatchType) {
    let ops = vec![
        (1, PLOperation::Lock(1, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(2, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Lock(4, (), l1_upgrade)),
        (2, PLOperation::Unlock(3, (), ())),
        (1, PLOperation::Unlock(5, (), ())),
    ];
    test_latch_operations(ops);
}

// l1 l2 non conflict
// l2 l1_upgrade no_conflict
fn test_upgrade_nc_nc(l1: PLatchType, l1_upgrade: PLatchType, l2: PLatchType) {
    let ops = vec![
        (1, PLOperation::Lock(1, (), l1)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Lock(2, (), l2)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Lock(3, (), l1_upgrade)),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (2, PLOperation::Unlock(4, (), ())),
        (1, PLOperation::Barrier),
        (2, PLOperation::Barrier),
        (1, PLOperation::Unlock(5, (), ())),
    ];
    test_latch_operations(ops);
}

fn set_log() {
    tracing_subscriber::fmt()
        // enable everything
        .with_max_level(tracing::Level::INFO)
        // display source code file paths
        .with_file(true)
        // display source code line numbers
        .with_line_number(true)
        // disable targets
        .with_target(false)
        // sets this to be the default, global collector for this application.
        .init();
}

fn test_lock_latch() {
    let lock_types = [
        PLatchType::WriteLock,
        PLatchType::ReadLock,
        PLatchType::AccessIntent,
        PLatchType::NodeDelete,
        PLatchType::ParentModification,
    ];
    for l1 in lock_types.iter() {
        for l2 in lock_types.iter() {
            let conflict = PLatchType::conflict(l1, l2);
            if conflict {
                test_conflict(l1.clone(), l2.clone());
            } else {
                test_non_conflict(l1.clone(), l2.clone());
            }
        }
    }
}

fn test_upgrade_latch() {
    let lock_types = [
        PLatchType::WriteLock,
        PLatchType::ReadLock,
        PLatchType::AccessIntent,
        PLatchType::NodeDelete,
        PLatchType::ParentModification,
    ];
    for l1 in lock_types.iter() {
        for l2 in lock_types.iter() {
            for l1_up in lock_types.iter() {
                let c = PLatchType::conflict(l1, l2);
                let c_up = PLatchType::conflict(l1_up, l2);
                let ok = match (l1, l1_up) {
                    (PLatchType::WriteLock, PLatchType::ParentModification) => true,
                    (PLatchType::AccessIntent, PLatchType::ReadLock) => true,
                    (PLatchType::AccessIntent, PLatchType::WriteLock) => true,
                    (_, _) => false,
                };
                if !ok {
                    continue;
                }
                trace!("LOCK {} {} {}", l1, l1_up, l2);
                if c && c_up {
                    test_upgrade_c_c(*l1, *l1_up, *l2)
                } else if !c && c_up {
                    test_upgrade_nc_c(*l1, *l1_up, *l2)
                } else if !c && !c_up {
                    test_upgrade_nc_nc(*l1, *l1_up, *l2)
                }
            }
        }
    }
}

#[test]
fn test_latch() {
    set_log();
    test_lock_latch();
    test_upgrade_latch()
}
