use std::ops::Bound;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_std::task;
use async_trait::async_trait;
use rand::rngs::ThreadRng;
use rand::Rng;
use scc::HashMap;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tracing::{info, trace};

use adt::range::RangeBounds;
use common::error_type::ET;
use common::id::{OID, XID};
use common::result::Result;
use storage::access::bt_pred::{BtPred, BtPredCtx};
use storage::trans::deadlock::DeadLockDetector;
use storage::trans::history::HistoryEmpty;
use storage::trans::lock_mgr::LockMgr;
use storage::trans::lock_slot::LockType;
use storage::trans::tx_lock::TxLock;
use storage::trans::tx_wait_notifier::TxWaitNotifier;
use storage::trans::victim_fn::VictimFn;

use crate::test_storage::gen_slice::{IntegerCompare, IntegerSlice, SliceStub};
use crate::test_storage::lock_test::{TLContext, TLOpParam, TLOps};

pub mod test_storage;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
enum TLockType {
    // (lock id, key)
    LockInitReadRow(u64),
    // (lock id, key)
    LockInitWriteRow(u64),
    // (lock id, (low key, high key))
    LockInitPredicate((u64, u64)),
    // (lock id, key, conflict predicate lock id)
    LockStepWriteRow(u64, OID),
    // (lock id, conflict key)
    LockStepPredicate(u64),
}

struct TxRoutine {
    xid: XID,
    notifier: Arc<TxWaitNotifier>,
    locks: HashMap<OID, Arc<TxLock>>,
}

#[derive(Clone)]
struct TxMap {
    tx: Arc<HashMap<OID, Arc<TxRoutine>>>,
}

type Cmp = IntegerCompare;

type TMyLockMgr = LockMgr<BtPredCtx<Cmp>, BtPred, HistoryEmpty>;
type TMyLockOp = TLOpParam<OID, TLockType, ()>;
type TMyLockOpList = TLOps<XID, OID, TLockType, ()>;
type TMyLockCtx = TLContext<XID, OID, TLockType, ()>;

impl TxRoutine {
    fn new(xid: XID) -> Self {
        Self {
            xid,
            notifier: Arc::new(TxWaitNotifier::new(xid)),
            locks: Default::default(),
        }
    }

    async fn run_tx_lock_op_list(
        &self,
        list: TMyLockOpList,
        mgr: &TMyLockMgr,
        ctx: &TMyLockCtx,
    ) -> Result<()> {
        for (_id, op) in list {
            trace!("run tx operation {} {:?}", _id, op);
            let result = self.run_tx_lock_op(op, mgr, ctx).await;
            match result {
                Err(e) => match e {
                    ET::Deadlock => {
                        return Ok(());
                    }
                    _ => {}
                },
                Ok(()) => {}
            }
        }
        Ok(())
    }

    async fn run_tx_lock_op(
        &self,
        op: TMyLockOp,
        mgr: &TMyLockMgr,
        ctx: &TMyLockCtx,
    ) -> Result<()> {
        match op {
            TLOpParam::Lock(_, id, t) => {
                self.lock(id, t, mgr).await?;
                ctx.append_history(self.xid, op).await;
            }
            TLOpParam::Unlock(_, id, _) => {
                self.unlock(id, mgr).await?;
                ctx.append_history(self.xid, op).await;
            }
            TLOpParam::Barrier => ctx.wait_barrier().await,
            TLOpParam::Sleep => {
                let mut rnd = ThreadRng::default();
                let ms = rnd.gen_range(0..100u64);
                task::sleep(Duration::from_millis(ms)).await;
            }
            _ => {
                panic!("TODO")
            }
        }
        Ok(())
    }

    async fn lock(&self, id: OID, lt: TLockType, mgr: &TMyLockMgr) -> Result<()> {
        match lt {
            TLockType::LockInitReadRow(key) => {
                let k = IntegerSlice::from_type(&key);
                let tx_lock = mgr
                    .lock_key_init(id, self.notifier.clone(), LockType::LockKeyRead, &k)
                    .await?;
                self.add_tx_lock(id, tx_lock).await;
            }
            TLockType::LockInitWriteRow(key) => {
                let k = IntegerSlice::from_type(&key);
                let tx_lock = mgr
                    .lock_key_init(id, self.notifier.clone(), LockType::LockKeyWrite, &k)
                    .await?;
                self.add_tx_lock(id, tx_lock).await;
            }
            TLockType::LockInitPredicate(range) => {
                let range = RangeBounds::new(
                    Bound::Included(IntegerSlice::from_type(&range.0)),
                    Bound::Included(IntegerSlice::from_type(&range.1)),
                );
                let pred = BtPred::from(&range);
                let tx_lock = mgr
                    .lock_predicate_init(id, self.notifier.clone(), &pred)
                    .await?;
                self.add_tx_lock(id, tx_lock).await;
            }
            TLockType::LockStepWriteRow(key, pred_id) => {
                let lock = self.get_tx_lock(id).await?;
                let k = IntegerSlice::from_type(&key);
                mgr.lock_key_step(&k, lock, pred_id).await?;
            }
            TLockType::LockStepPredicate(key) => {
                let lock = self.get_tx_lock(id).await?;
                let k = IntegerSlice::from_type(&key);
                mgr.lock_predicate_step(lock, &k).await?;
            }
        }
        Ok(())
    }

    async fn unlock(&self, id: OID, mgr: &TMyLockMgr) -> Result<()> {
        let lock = self.get_tx_lock(id).await?;
        mgr.unlock(lock).await;
        Ok(())
    }

    async fn get_tx_lock(&self, lock_id: OID) -> Result<Arc<TxLock>> {
        let opt = self.locks.read_async(&lock_id, |_, v| v.clone()).await;
        match opt {
            Some(a) => Ok(a),
            None => Err(ET::NoSuchElement),
        }
    }

    async fn add_tx_lock(&self, id: OID, l: Arc<TxLock>) {
        let _ = self.locks.insert_async(id, l).await;
    }

    async fn victim_abort(&self) {
        self.notifier.notify_abort(Err(ET::Deadlock)).await
    }
}

#[async_trait]
impl VictimFn for TxMap {
    async fn victim(&self, xid: XID) {
        let opt_tx = self.tx.read_async(&xid, |_, v| v.clone()).await;
        match opt_tx {
            Some(t) => {
                t.victim_abort().await;
            }
            None => {}
        }
    }
}

impl TxMap {
    fn new() -> Self {
        Self {
            tx: Default::default(),
        }
    }

    async fn add_tx(&self, tx: Arc<TxRoutine>) {
        let _ = self.tx.insert_async(tx.xid, tx).await;
    }
}

fn test_lock(list: TMyLockOpList, deadlock: bool) {
    let test_lock_ctx = Arc::new(TMyLockCtx::new(list));
    let xid2ops_map = test_lock_ctx.id_2_lock_ops_map().clone();
    let tx_map1 = TxMap::new();
    let tx_map2 = tx_map1.clone();
    let duration = if deadlock {
        Duration::from_millis(100)
    } else {
        // a long detection timeout duration
        // for debug
        Duration::from_secs(100)
    };
    let dl1 = Arc::new(DeadLockDetector::new(duration));
    let dl2 = dl1.clone();
    let dl_thread = thread::spawn(move || {
        let t = LocalSet::new();
        let _ = t.spawn_local(async move {
            let _ = dl2.detect(tx_map2).await;
        });
        let r = Builder::new_current_thread().enable_all().build().unwrap();
        r.block_on(t);
    });
    let lock_mgr = Arc::new(TMyLockMgr::new(0, dl1.clone(), HistoryEmpty::new()));
    let local = LocalSet::new();
    for (xid, ops) in xid2ops_map {
        let mgr = lock_mgr.clone();
        let ctx = test_lock_ctx.clone();
        let tx_map = tx_map1.clone();
        let _ = local.spawn_local(async move {
            let routine = Arc::new(TxRoutine::new(xid));
            let _ = tx_map.add_tx(routine.clone()).await;
            trace!("run tx lock operation list, {}", xid);
            let result = routine.run_tx_lock_op_list(ops, &mgr, &ctx).await;
            assert!(result.is_ok());
        });
    }

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(local);

    let check_task = LocalSet::new();
    check_task.spawn_local(async move {
        dl1.stop().await;
        if !deadlock {
            test_lock_ctx.check_history().await;
        }
    });
    runtime.block_on(check_task);

    // wait deadlock detection thread stop
    let _ = dl_thread.join();
}

fn case_deadlock() -> TMyLockOpList {
    info!("generate lock deadlock test case");
    vec![
        // tx x1            tx x2
        // x1 l1 101
        // barrier          barrier
        (1, TMyLockOp::Lock(1, 101, TLockType::LockInitReadRow(1))),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(2, 201, TLockType::LockInitReadRow(2))),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Lock(3, 102, TLockType::LockInitWriteRow(2))),
        (2, TMyLockOp::Sleep),
        (2, TMyLockOp::Lock(4, 202, TLockType::LockInitWriteRow(1))),
    ]
}

fn case_key_non_conflict(l1: TLockType, l2: TLockType) -> TMyLockOpList {
    info!("generate lock non-conflict test case {:?} {:?}", l1, l2);
    vec![
        // tx x1            tx x2
        // x1 l1 101
        // barrier          barrier
        //                  x2 l2 201
        // barrier          barrier
        //                  x2 ul 201
        // barrier          barrier
        // x1 ul 102
        // barrier          barrier
        // x1 l1 102
        // barrier          barrier
        //                  x2 l2 202
        // barrier          barrier
        //                  x2 ul 202
        // barrier          barrier
        // x1 ul 102
        (1, TMyLockOp::Lock(1, 101, l1)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(2, 201, l2)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Unlock(3, 201, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Unlock(4, 101, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Lock(5, 102, l1)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(6, 202, l2)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Unlock(7, 202, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Unlock(8, 102, ())),
    ]
}

fn case_key_conflict(l1: TLockType, l2: TLockType) -> TMyLockOpList {
    info!("generate lock conflict test case {:?} {:?}", l1, l2);
    vec![
        // tx x1            tx x2
        // x1 l1 101
        // barrier          barrier
        // sleep
        // x1 ul 101
        //                  x2 l2 201
        //                  x2 ul 201
        // barrier          barrier
        //                  x2 l2 202
        // barrier          barrier
        //                  sleep
        //                  x2 ul 202
        // x1 l1 102
        // x1 ul 102
        (1, TMyLockOp::Lock(1, 101, l1)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(3, 201, l2)),
        (2, TMyLockOp::Unlock(4, 201, ())),
        (1, TMyLockOp::Sleep),
        (1, TMyLockOp::Unlock(2, 101, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(5, 202, l2)),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Lock(7, 102, l1)),
        (1, TMyLockOp::Unlock(8, 102, ())),
        (2, TMyLockOp::Sleep),
        (2, TMyLockOp::Unlock(6, 202, ())),
    ]
}

fn case_key_conflict_with_predicate() -> TMyLockOpList {
    info!("generate key conflict with predicate test case");
    // tx x1            tx x2
    // x1 pl_i 101
    // barrier          barrier
    //                  x2 wl_i 201
    // barrier          barrier
    // x1 sleep
    // x1 ul 101
    //                  x2 wl_s 201
    // barrier          barrier
    //                  x2 ul 201
    // barrier          barrier
    // x1 pl_i 102
    // x1 pl_s 102
    // x1 ul 102
    vec![
        (
            1,
            TMyLockOp::Lock(1, 101, TLockType::LockInitPredicate((0, 10))),
        ),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Lock(2, 201, TLockType::LockInitWriteRow(1))),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Sleep),
        (1, TMyLockOp::Unlock(3, 101, ())),
        (
            2,
            TMyLockOp::Lock(4, 201, TLockType::LockStepWriteRow(1, 101)),
        ),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Unlock(5, 201, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (
            1,
            TMyLockOp::Lock(6, 102, TLockType::LockInitPredicate((0, 10))),
        ),
        (1, TMyLockOp::Lock(7, 102, TLockType::LockStepPredicate(1))),
        (1, TMyLockOp::Unlock(8, 102, ())),
    ]
}

fn case_predicate_conflict_with_key() -> TMyLockOpList {
    info!("generate predicate conflict with key test case");
    vec![
        // tx x1            tx x2
        // x1 wl_i 101
        // barrier          barrier
        //                  x2 pl_i 201
        // barrier          barrier
        // x1 sleep
        // x1 ul 101
        //                  x2 pl_s 201
        // barrier          barrier
        //                  x2 ul 201
        // barrier          barrier
        // x1 l1 102
        // x1 ul 102
        (1, TMyLockOp::Lock(1, 101, TLockType::LockInitWriteRow(1))),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (
            2,
            TMyLockOp::Lock(2, 201, TLockType::LockInitPredicate((0, 10))),
        ),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (1, TMyLockOp::Sleep),
        (1, TMyLockOp::Unlock(3, 101, ())),
        (2, TMyLockOp::Lock(4, 201, TLockType::LockStepPredicate(1))),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        (2, TMyLockOp::Unlock(5, 201, ())),
        (1, TMyLockOp::Barrier),
        (2, TMyLockOp::Barrier),
        // should not blocking, if blocking here, predicate unlock has bugs,
        (1, TMyLockOp::Lock(6, 102, TLockType::LockInitWriteRow(1))),
        (1, TMyLockOp::Unlock(7, 102, ())),
    ]
}

fn test_cases_deadlock() -> Vec<TMyLockOpList> {
    vec![case_deadlock()]
}

fn test_cases_non_deadlock() -> Vec<TMyLockOpList> {
    vec![
        case_key_non_conflict(TLockType::LockInitReadRow(1), TLockType::LockInitReadRow(2)),
        case_key_non_conflict(
            TLockType::LockInitReadRow(1),
            TLockType::LockInitWriteRow(2),
        ),
        case_key_non_conflict(
            TLockType::LockInitWriteRow(1),
            TLockType::LockInitReadRow(2),
        ),
        case_key_conflict(
            TLockType::LockInitWriteRow(1),
            TLockType::LockInitWriteRow(1),
        ),
        case_key_conflict(
            TLockType::LockInitReadRow(1),
            TLockType::LockInitWriteRow(1),
        ),
        case_key_conflict(
            TLockType::LockInitWriteRow(1),
            TLockType::LockInitReadRow(1),
        ),
        case_key_conflict_with_predicate(),
        case_predicate_conflict_with_key(),
    ]
}

#[test]
fn test_tx_lock() {
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

    for case in test_cases_non_deadlock() {
        test_lock(case, false);
    }

    for case in test_cases_deadlock() {
        test_lock(case, true);
    }
}
