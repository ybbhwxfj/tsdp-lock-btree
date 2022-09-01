/// Util function for testing lock && latch
/// Test case predefine some operation sequence by LockOp
/// The test program would execute the sequence and check the history
/// If  1. deadlock occur, the test program would not stop;
///     2. or the check result is not consistency with the predefined order.
///     then there are bugs
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use async_std::sync::Mutex;
use serde::Serialize;
use tokio::sync::Barrier;
use tokio::sync::RwLock;
use tracing::{debug, error};

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize)]
pub enum TLOp {
    Lock,
    Upgrade,
    Unlock,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
pub enum TLOpParam<
    LID: PartialEq + Eq + Hash + Clone + Debug + Copy,
    L: PartialEq + Eq + Hash + Clone + Debug + Copy,
    U: PartialEq + Eq + Hash + Clone + Debug + Copy,
> {
    // (sequence order, lock id, parameter lock )
    Lock(u32, LID, L),
    Upgrade(u32, LID, L),
    // (sequence order, lock id, parameter for unlock)
    Unlock(u32, LID, U),
    Barrier,
    Sleep,
}

pub type TLOps<
    RID, // routine id(or transaction id)
    LID, // task id
    L,   // lock parameter
    U,   // unlock parameter
> = Vec<(RID, TLOpParam<LID, L, U>)>;

#[derive(Clone)]
pub struct TLContext<
    RID: PartialEq + Eq + Hash + Clone + Debug + Copy,
    LID: PartialEq + Eq + Hash + Clone + Debug + Copy,
    L: PartialEq + Eq + Hash + Clone + Debug + Copy,
    U: PartialEq + Eq + Hash + Clone + Debug + Copy,
> {
    all_lock_ops: TLOps<RID, LID, L, U>,
    id_2_lock_ops: HashMap<RID, TLOps<RID, LID, L, U>>,

    history: Arc<Mutex<TLOps<RID, LID, L, U>>>,

    barrier: TLBarrier,
}

#[derive(Clone)]
pub struct TLBarrier {
    concurrent: usize,
    barrier: Arc<RwLock<Cell<Barrier>>>,
}

impl TLBarrier {
    pub fn new(concurrent: usize) -> Self {
        Self {
            concurrent,
            barrier: Arc::new(RwLock::new(Cell::new(Barrier::new(concurrent)))),
        }
    }

    pub async fn wait_barrier(&self) {
        let is_leader = {
            let read_cell = self.barrier.read().await;
            let barrier = unsafe { &*read_cell.as_ptr() };
            let r = barrier.wait().await;
            r.is_leader()
        };
        if is_leader {
            let write_cell = self.barrier.write().await;
            // reset the barrier
            write_cell.set(Barrier::new(self.concurrent))
        }
    }
}

impl<
        RID: PartialEq + Eq + Hash + Clone + Debug + Copy,
        LID: PartialEq + Eq + Hash + Clone + Debug + Copy,
        L: PartialEq + Eq + Hash + Clone + Debug + Copy,
        U: PartialEq + Eq + Hash + Clone + Debug + Copy,
    > TLContext<RID, LID, L, U>
{
    pub fn new(lock_ops: TLOps<RID, LID, L, U>) -> Self {
        let map = Self::group_by_id(&lock_ops);
        let concurrent = map.len();
        Self {
            all_lock_ops: lock_ops,
            id_2_lock_ops: map,

            history: Arc::new(Default::default()),
            barrier: TLBarrier::new(concurrent),
        }
    }
    #[allow(dead_code)]
    pub fn id_2_lock_ops_map(&self) -> &HashMap<RID, TLOps<RID, LID, L, U>> {
        &self.id_2_lock_ops
    }

    pub async fn append_history(&self, id: RID, op: TLOpParam<LID, L, U>) {
        let mut history = self.history.lock().await;
        let seq = history.len() as u32;
        let lock_op = match op {
            TLOpParam::Lock(_, lid, l) => TLOpParam::Lock(seq, lid, l),
            TLOpParam::Unlock(_, lid, u) => TLOpParam::Unlock(seq, lid, u),
            _ => {
                panic!("error!")
            }
        };
        history.push((id, lock_op));
    }
    #[allow(dead_code)]
    pub async fn check_history(&self) {
        debug!("begin check history");
        // we first exclude the barrier wait operations
        let mut ops_reorder = Self::exclude_non_lock(&self.all_lock_ops);
        // reorder this by sequence number ...
        ops_reorder.sort_by(|(_, op1), (_, op2)| {
            let i1 = match op1 {
                TLOpParam::Lock(i, _, _) => i,
                TLOpParam::Unlock(i, _, _) => i,
                _ => {
                    panic!("error")
                }
            };
            let i2 = match op2 {
                TLOpParam::Lock(i, _, _) => i,
                TLOpParam::Unlock(i, _, _) => i,
                _ => {
                    panic!("error")
                }
            };
            i1.cmp(&i2)
        });

        let history = self.history.lock().await;
        if history.len() != ops_reorder.len() {
            error!("wrong history, {} operations, {:?}", history.len(), history);
            error!(
                "correct history, {} operations,  {:?}",
                ops_reorder.len(),
                ops_reorder
            );
        }

        // check the history is consistent with the right order
        assert_eq!(history.len(), ops_reorder.len());
        for (i, (id, op)) in ops_reorder.iter().enumerate() {
            let opt = history.get(i);
            match opt {
                Some((h_id, h_op)) => {
                    assert_eq!(h_id, id);
                    match (op, h_op) {
                        (TLOpParam::Lock(_, _, l1), TLOpParam::Lock(_, _, l2)) => {
                            assert_eq!(l1, l2);
                        }
                        (TLOpParam::Unlock(_, _, u1), TLOpParam::Unlock(_, _, u2)) => {
                            assert_eq!(u1, u2);
                        }
                        _ => {
                            assert!(false);
                        }
                    }
                }
                None => {
                    assert!(false);
                }
            }
        }
        debug!("end check history");
    }
    #[allow(dead_code)]
    pub async fn wait_barrier(&self) {
        self.barrier.wait_barrier().await;
    }

    #[allow(dead_code)]
    fn group_by_id(ops: &TLOps<RID, LID, L, U>) -> HashMap<RID, TLOps<RID, LID, L, U>> {
        let mut set: HashMap<RID, TLOps<RID, LID, L, U>> = HashMap::new();

        for (i, op) in ops.iter() {
            let opt = set.get_mut(i);
            match opt {
                Some(list) => {
                    list.push((*i, op.clone()));
                }
                None => {
                    let mut list = TLOps::<RID, LID, L, U>::new();
                    list.push((*i, op.clone()));
                    set.insert(*i, list);
                }
            }
        }
        set
    }
    #[allow(dead_code)]
    fn exclude_non_lock(ops: &TLOps<RID, LID, L, U>) -> TLOps<RID, LID, L, U> {
        let mut vec = vec![];
        for (i, op) in ops.iter() {
            match op {
                TLOpParam::Lock(_, _, _) | TLOpParam::Unlock(_, _, _) => {
                    vec.push((i.clone(), op.clone()))
                }
                _ => {}
            }
        }
        vec
    }
}
