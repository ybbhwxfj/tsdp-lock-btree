use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_std::future::timeout;
use std::time::Duration;

use crate::test_storage::lock_test::TLOp;
use async_std::task;
use atomic::Atomic;
use common::error_type::ET;
use common::id::TableId;
use common::result::Result;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use scc::ebr::Barrier;
use scc::HashIndex;
use serde::Serialize;
use storage::access::file_id::FileId;
use storage::access::latch_type::PLatchType;
use storage::access::page_id::{PageId, PageIndex};
use storage::access::page_latch::PageLatchEP;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tracing::{error, info};

pub mod test_storage;

const TEST_FILE_ID: FileId = 0;

#[derive(Clone, Serialize, Eq, PartialEq, Hash)]
struct TLatchOp {
    task_id: u64,
    sequence_id: u64,
    lock_key: u64,
    operation: TLOp,
    latch_type: PLatchType,
}

#[derive(Serialize, Clone, Copy, Debug)]
enum THistoryOpType {
    THOBefore,
    THOAfter,
    THOTimeout,
}

#[derive(Clone)]
struct TLConTestCtx {
    latch: Arc<HashIndex<u64, PageLatchEP>>,
    timestamp: Arc<Atomic<u64>>,
    history: Arc<HashIndex<u64, (THistoryOpType, TLatchOp)>>,
}

impl TLatchOp {
    fn new(
        task_id: u64,
        sequence_id: u64,
        lock_key: u64,
        operation: TLOp,
        latch_type: PLatchType,
    ) -> Self {
        Self {
            task_id,
            sequence_id,
            lock_key,
            operation,
            latch_type,
        }
    }
}

impl TLConTestCtx {
    fn new() -> Self {
        Self {
            latch: Arc::new(Default::default()),
            timestamp: Arc::new(Default::default()),
            history: Arc::new(Default::default()),
        }
    }

    async fn add_timeout_op(&self, op: TLatchOp) {
        let timestamp = self.timestamp.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .history
            .insert_async(timestamp, (THistoryOpType::THOTimeout, op));
    }

    async fn append_before(&self, op: TLatchOp) {
        let timestamp = self.timestamp.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .history
            .insert_async(timestamp, (THistoryOpType::THOBefore, op))
            .await;
    }

    async fn append_after(&self, op: TLatchOp) {
        let timestamp = self.timestamp.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .history
            .insert_async(timestamp, (THistoryOpType::THOAfter, op))
            .await;
    }

    fn hash_index_to_vec<K: 'static + Clone + Eq + Hash + Sync + Ord, V: 'static + Clone + Sync>(
        hash_index: &HashIndex<K, V>,
    ) -> Vec<(K, V)> {
        let barrier = Barrier::new();
        let mut vec = Vec::new();
        for (k, v) in hash_index.iter(&barrier) {
            vec.push((k.clone(), v.clone()));
        }
        vec.sort_by(|(k1, _), (k2, _)| -> std::cmp::Ordering {
            return k1.cmp(k2);
        });
        vec
    }

    async fn check_conflict_operation(
        &self,
        ops: &HashMap<(u64, u64), TLatchOp>,
        op: &TLatchOp,
        history_to_check: &Vec<(u64, (THistoryOpType, TLatchOp))>,
        correct_is_conflict: bool,
    ) -> bool {
        for (_, v) in ops {
            if v.task_id == op.task_id {
                continue;
            }
            if v.lock_key != op.lock_key {
                continue;
            }
            let conflict = PLatchType::conflict(&v.latch_type, &op.latch_type);

            if conflict {
                if !correct_is_conflict {
                    error!("history:");
                    error!("---------------------");
                    error!(
                        "{}",
                        serde_json::to_string(&history_to_check)
                            .unwrap()
                            .to_string()
                    );
                    {
                        let b = Barrier::new();
                        let mut vec = Vec::new();
                        for (ts, l) in self.latch.iter(&b) {
                            vec.push((ts.clone(), l.to_debug_string().await));
                        }
                        for (ts, s) in vec {
                            error!("ts {}, {}", ts, s);
                        }
                    }
                    assert!(correct_is_conflict);
                }
                return true;
            }
        }
        false
    }

    // return value,
    //  (
    //      timestamp:u64,
    //      is before schedule operation : bool,
    //      operation payload,
    //  )

    fn history_to_check(&self) -> Vec<(u64, (THistoryOpType, TLatchOp))> {
        let history = Self::hash_index_to_vec(&self.history);
        history
    }

    async fn check_latch_correct(&self) {
        let history_to_check = self.history_to_check();
        let mut latch_table: HashMap<u64, HashMap<(u64, u64), TLatchOp>> = HashMap::new();
        for (_ts, (h_t, op)) in &history_to_check {
            match h_t {
                THistoryOpType::THOBefore => match op.operation {
                    TLOp::Upgrade => {
                        if latch_table.contains_key(&op.lock_key) {
                            let slot = latch_table.get_mut(&op.lock_key).unwrap();
                            let opt = slot.remove(&(op.task_id, op.sequence_id));
                            assert!(opt.is_some());
                        }
                    }
                    _ => {}
                },
                THistoryOpType::THOAfter => match op.operation {
                    TLOp::Lock | TLOp::Upgrade => {
                        if !latch_table.contains_key(&op.lock_key) {
                            let mut slot = HashMap::new();
                            let _ = slot.insert((op.task_id, op.sequence_id), op.clone());
                            let _ = latch_table.insert(op.lock_key, slot);
                        } else {
                            let conflict = {
                                let slot = latch_table.get_mut(&op.lock_key).unwrap();
                                let _ = slot.insert((op.task_id, op.sequence_id), op.clone());
                                let conflict = self
                                    .check_conflict_operation(&slot, &op, &history_to_check, false)
                                    .await;
                                conflict
                            };
                            assert!(!conflict);
                        }
                    }
                    TLOp::Unlock => {
                        if latch_table.contains_key(&op.lock_key) {
                            let slot = latch_table.get_mut(&op.lock_key).unwrap();
                            let opt = slot.remove(&(op.task_id, op.sequence_id));
                            assert!(opt.is_some());
                        }
                    }
                },
                THistoryOpType::THOTimeout => {
                    match op.operation {
                        TLOp::Lock | TLOp::Upgrade => {
                            if !latch_table.contains_key(&op.lock_key) {
                                panic!("error, no such key, lock timeout")
                            } else {
                                let conflict = {
                                    let slot = latch_table.get_mut(&op.lock_key).unwrap();
                                    let conflict = self
                                        .check_conflict_operation(
                                            &slot,
                                            &op,
                                            &history_to_check,
                                            true,
                                        )
                                        .await;
                                    conflict
                                };
                                // must be some conflict latch
                                assert!(conflict);
                            }
                        }
                        TLOp::Unlock => {
                            panic!("error, unlock timeout");
                        }
                    }
                }
            }
        }
    }
}

async fn test_latch_task(
    test: TLConTestCtx,
    task_id: u64,
    num_max_page: u64,
    num_latch_op: u64,
    latch_hold_millis: u64,
    timeout_millis: u64,
) -> Result<()> {
    let list = [
        (PLatchType::WriteLock, None),
        (PLatchType::ReadLock, None),
        (PLatchType::AccessIntent, None),
        (PLatchType::ParentModification, None),
        (PLatchType::NodeDelete, None),
        (PLatchType::WriteLock, Some(PLatchType::ParentModification)),
        (PLatchType::WriteLock, Some(PLatchType::NodeDelete)),
        (PLatchType::AccessIntent, Some(PLatchType::ReadLock)),
        (PLatchType::AccessIntent, Some(PLatchType::WriteLock)),
    ];
    let mut rnd = ThreadRng::default();

    for sequence in 0..num_latch_op {
        let (l_type, opt_upgrade) = list.choose(&mut rnd).unwrap();
        let key = rnd.gen_range(0..num_max_page);

        let barrier = Barrier::new();
        let opt = test.latch.read_with(&key, |_, v| v.clone(), &barrier);
        let latch = match opt {
            Some(latch) => latch,
            _ => {
                panic!("ERROR")
            }
        };
        let op_lock = TLatchOp::new(task_id, sequence, key, TLOp::Lock, l_type.clone());
        test.append_before(op_lock.clone()).await;
        let f_lock = latch.lock(l_type.clone(), (task_id * 1000000 + sequence) as TableId);
        let result = timeout(Duration::from_millis(timeout_millis), f_lock).await;
        let mut guard = match result {
            Ok(g) => g,
            Err(_e) => {
                test.add_timeout_op(op_lock.clone()).await;
                return Err(ET::Timeout);
            }
        };
        test.append_after(op_lock.clone()).await;
        let ul_type = match opt_upgrade {
            Some(l_type_up) => {
                let op_upgrade_lock =
                    TLatchOp::new(task_id, sequence, key, TLOp::Upgrade, l_type_up.clone());
                test.append_before(op_upgrade_lock.clone()).await;
                let f_lock_upgrade = guard.upgrade(l_type_up.clone());
                let result_timeout_upgrade =
                    timeout(Duration::from_millis(timeout_millis), f_lock_upgrade).await;
                match result_timeout_upgrade {
                    Ok(g) => {
                        guard = g;
                    }
                    Err(_e) => {
                        guard.unlock().await;
                        test.add_timeout_op(op_upgrade_lock.clone()).await;
                        return Err(ET::Timeout);
                    }
                };
                test.append_after(op_upgrade_lock.clone()).await;
                l_type_up
            }
            _ => l_type,
        };

        task::sleep(Duration::from_millis(latch_hold_millis)).await;
        let op_unlock = TLatchOp::new(task_id, sequence, key, TLOp::Unlock, ul_type.clone());
        test.append_before(op_unlock.clone()).await;
        let f_unlock = guard.unlock();
        let result_unlock = timeout(Duration::from_millis(timeout_millis), f_unlock).await;
        match result_unlock {
            Ok(_) => {}
            Err(_e) => {
                guard.unlock().await;
                test.add_timeout_op(op_unlock.clone()).await;
                return Err(ET::Timeout);
            }
        };
        test.append_after(op_unlock.clone()).await;
    }
    Ok(())
}

fn test_latch_concurrency(
    num_max_page: u64,
    num_concurrency: u64,
    num_latch_op: u64,
    latch_hold_millis: u64,
    timeout_millis: u64,
) {
    let test = TLConTestCtx::new();
    for i in 0..num_max_page {
        let l = PageLatchEP::new(
            PageIndex::new(TEST_FILE_ID, i as PageId),
            Vec::new(),
            Box::new(0u32),
        );
        let _ = test.latch.insert(i, l);
    }
    let local = LocalSet::new();
    for task_id in 0..num_concurrency {
        let test2 = test.clone();
        let _ = local.spawn_local(async move {
            let r = test_latch_task(
                test2,
                task_id,
                num_max_page,
                num_latch_op,
                latch_hold_millis,
                timeout_millis,
            )
            .await;
            if r.is_err() {}
        });
    }
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(local);

    let local_check = LocalSet::new();
    local_check.spawn_local(async move {
        let _ = test.check_latch_correct().await;
    });

    runtime.block_on(local_check);
}

#[test]
fn test_concurrency_latch() {
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

    let num_max_page = 1;
    let num_latch_op = 100;
    let num_concurrency = 10;
    let latch_hold_millis = 10;
    let timeout_millis = 10000;
    // 10s
    info!("test concurrency latch");
    test_latch_concurrency(
        num_max_page,
        num_concurrency,
        num_latch_op,
        latch_hold_millis,
        timeout_millis,
    );
}
