use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use async_std::future::timeout;
use async_std::sync::Mutex;
use atomic::Atomic;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use common::error_type::ET;
use common::id::{INVALID_OID, OID, XID};
use common::result::Result;

use crate::trans::victim_fn::VictimFn;

#[derive(Clone, Copy, Eq, PartialEq)]
enum WalkState {
    ColorGray,
    ColorWhite,
    ColorBlack,
}

#[derive(Serialize, Deserialize, Default)]
pub struct TxWait {
    out: HashSet<XID>,
    // Never serialized.
    #[serde(skip)]
    state: Atomic<WalkState>,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct TxWaitSet {
    tx_wait_set: HashMap<OID, TxWait>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DebugDeadlock {
    pub dependency: HashMap<OID, HashSet<OID>>,
    pub circle: HashSet<Vec<OID>>,
}

enum DLDOperation {
    DLDAddDependency(XID, XID),
    DLDRemove(XID),
    DLDStop,
}

pub struct DeadLockDetector {
    recv: Receiver<DLDOperation>,
    send: Sender<DLDOperation>,
    wait: Mutex<TxWaitSet>,
    duration: Duration,
}

impl Default for WalkState {
    fn default() -> Self {
        WalkState::ColorWhite
    }
}

impl TxWait {
    pub fn new() -> Self {
        Self {
            out: Default::default(),
            state: Atomic::new(WalkState::ColorWhite),
        }
    }
}

impl Clone for TxWait {
    fn clone(&self) -> Self {
        Self {
            out: self.out.clone(),
            state: Atomic::new(self.state.load(Ordering::SeqCst)),
        }
    }
}

impl TxWaitSet {
    pub fn new() -> Self {
        Self {
            tx_wait_set: Default::default(),
        }
    }

    pub fn clear(&mut self) {
        self.tx_wait_set.clear();
    }

    pub fn remove(&mut self, xid: XID) {
        let _ = self.tx_wait_set.remove(&xid);
    }

    pub fn add(&mut self, in_id: XID, out_id: XID) {
        let opt = self.tx_wait_set.get_mut(&in_id);
        match opt {
            Some(w) => {
                let _ = w.out.insert(out_id);
            }
            None => {
                let mut wait = TxWait::new();
                wait.out.insert(out_id);
                self.tx_wait_set.insert(in_id, wait);
            }
        }
        // no such transaction out
        if !self.tx_wait_set.contains_key(&out_id) {
            self.tx_wait_set.insert(out_id, TxWait::new());
        }
    }

    pub fn detect_circle(&self, circle: &mut HashSet<Vec<XID>>) {
        for (_, wait) in self.tx_wait_set.iter() {
            wait.state.store(WalkState::ColorWhite, Ordering::Relaxed);
        }
        let mut path = Vec::new();
        for (xid, wait) in &self.tx_wait_set {
            Self::detect_circle_recursive(&self.tx_wait_set, *xid, wait, &mut path, circle)
        }
    }

    fn detect_circle_recursive(
        tx_wait_set: &HashMap<XID, TxWait>,
        xid: OID,
        tx_wait: &TxWait,
        path: &mut Vec<XID>,
        circle: &mut HashSet<Vec<XID>>,
    ) {
        if tx_wait.state.load(Ordering::Relaxed) != WalkState::ColorWhite {
            return;
        }

        tx_wait.state.store(WalkState::ColorGray, Ordering::Relaxed);
        path.push(xid);

        for out_xid in &tx_wait.out {
            let opt = tx_wait_set.get(out_xid);
            let out_tx = match opt {
                Some(x) => x,
                None => {
                    continue;
                }
            };
            match out_tx.state.load(Ordering::Relaxed) {
                WalkState::ColorWhite => {
                    Self::detect_circle_recursive(tx_wait_set, *out_xid, out_tx, path, circle);
                }
                WalkState::ColorGray => {
                    let mut index = path.len();
                    for (i, x) in path.iter().enumerate() {
                        if *x == *out_xid {
                            index = i;
                            break;
                        }
                    }
                    if index == path.len() {
                        panic!("error");
                    }
                    let vec = path[index..].to_vec();
                    let _ = circle.insert(vec);
                }
                WalkState::ColorBlack => {
                    //out_tx.out.remove(out_xid);
                }
            }
        }
        tx_wait
            .state
            .store(WalkState::ColorBlack, Ordering::Relaxed);
        let _ = path.pop();
    }
}

impl DeadLockDetector {
    pub fn new(duration: Duration) -> Self {
        let (send, recv) = async_channel::bounded(10000);

        Self {
            recv,
            send,
            wait: Mutex::new(TxWaitSet::new()),
            duration,
        }
    }
    pub fn duration_detect(&self) -> Duration {
        self.duration
    }

    // only for debug
    pub async fn debug_deadlock(&self) -> DebugDeadlock {
        let mut dependency = HashMap::new();
        let mut circle = HashSet::new();
        let wait_for_set = self.wait.lock().await;
        for (xid, w) in &wait_for_set.tx_wait_set {
            let out = w.out.clone();
            let _ = dependency.insert(xid.clone(), out);
        }
        wait_for_set.detect_circle(&mut circle);

        DebugDeadlock { dependency, circle }
    }

    pub async fn add_dependency(&self, in_xid: XID, out_xid: XID) -> Result<()> {
        let r = self
            .send
            .send(DLDOperation::DLDAddDependency(in_xid, out_xid))
            .await;
        match r {
            Ok(_) => Ok(()),
            Err(_e) => Err(ET::ChSendError),
        }
    }

    pub async fn remove(&self, xid: XID) -> Result<()> {
        let r = self.send.send(DLDOperation::DLDRemove(xid)).await;
        match r {
            Ok(_) => Ok(()),
            Err(_e) => Err(ET::ChSendError),
        }
    }

    #[cfg_attr(debug_assertions, inline(never))]
    pub async fn detect<V: VictimFn>(&self, victim_fn: V) -> Result<()> {
        let ins = time::Instant::now();
        let mut detect_count = 0u64;
        let mut rnd = ThreadRng::default();
        let mut circle = HashSet::<Vec<XID>>::new();
        loop {
            let f0 = self.recv.recv();
            let result = timeout(Duration::from_millis(500), f0).await;
            match result {
                Ok(result_recv) => match result_recv {
                    Ok(op) => match op {
                        DLDOperation::DLDAddDependency(in_id, out_id) => {
                            self.dld_add_dependency(
                                in_id,
                                out_id,
                                &ins,
                                &mut detect_count,
                                &mut circle,
                            )
                            .await;
                        }
                        DLDOperation::DLDRemove(id) => {
                            self.dld_remove(id).await;
                        }
                        DLDOperation::DLDStop => {
                            break;
                        }
                    },
                    Err(_e) => {
                        return Err(ET::ChRecvError);
                    }
                },
                Err(_e) => {
                    let wait = self.wait.lock().await;
                    wait.detect_circle(&mut circle);
                }
            }
            if !circle.is_empty() {
                for c in &circle {
                    let to_victim = c.choose(&mut rnd);
                    match to_victim {
                        Some(x) => {
                            victim_fn.victim(*x).await;
                        }
                        _ => {}
                    }
                }
                circle.clear();
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        // send stop condition
        let _ = self.send.send(DLDOperation::DLDStop).await;
    }

    async fn dld_add_dependency(
        &self,
        in_id: XID,
        out_id: XID,
        ins: &time::Instant,
        count: &mut u64,
        circle: &mut HashSet<Vec<XID>>,
    ) {
        if in_id == INVALID_OID || out_id == INVALID_OID {
            // stop condition
            return;
        }
        let mut wait = self.wait.lock().await;
        wait.add(in_id, out_id);
        let millis = ins.elapsed().as_millis();
        if millis > (*count * 1000) as u128 {
            wait.detect_circle(circle);
            *count = (*count).wrapping_add(1);
        }
    }

    async fn dld_remove(&self, id: XID) {
        let mut wait = self.wait.lock().await;
        wait.remove(id);
    }
}
