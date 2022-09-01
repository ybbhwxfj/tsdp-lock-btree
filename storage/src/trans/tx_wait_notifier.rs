use async_std::sync::{Condvar, Mutex};
use log::debug;
use tracing::error;

use common::id::XID;
use common::result::Result;

use crate::trans::tx_state::TxState;

struct WaitCond {
    log_flush: bool,
    lock_wait_count: u64,
    tx_state: TxState,
    result: Result<()>,
}

pub struct TxWaitNotifier {
    xid: XID,
    wait_cond: Mutex<WaitCond>,
    cond: Condvar,
}

impl WaitCond {
    fn new() -> Self {
        Self {
            log_flush: false,
            lock_wait_count: 0,
            tx_state: TxState::TxIdle,
            result: Ok(()),
        }
    }
}

impl TxWaitNotifier {
    pub fn new(xid: XID) -> TxWaitNotifier {
        TxWaitNotifier {
            xid,
            wait_cond: Mutex::new(WaitCond::new()),
            cond: Default::default(),
        }
    }

    pub fn xid(&self) -> XID {
        self.xid
    }

    pub async fn lock_increase_wait(&self, n: u64) {
        let mut notified = self.wait_cond.lock().await;
        notified.lock_wait_count += n;
    }

    pub async fn lock_wait_lock(&self) -> Result<()> {
        debug!("tx lock wait");
        let mut notified = self.wait_cond.lock().await;
        while notified.tx_state.is_running() && notified.lock_wait_count != 0 {
            match notified.tx_state {
                TxState::TxIdle => {
                    notified.tx_state = TxState::TxWaitingLock;
                }
                TxState::TxWaitingLock => {}
                _ => {
                    error!("tx state, {:?}", notified.tx_state);
                    panic!("error");
                }
            }
            notified = self.cond.wait(notified).await;
        }

        match notified.tx_state {
            TxState::TxWaitingLock => {
                notified.tx_state = TxState::TxIdle;
            }
            TxState::TxAborted => {
                debug!("abort tx");
                return notified.result.clone();
            }
            _ => {}
        }
        debug!("acquire lock");
        Ok(())
    }

    pub async fn lock_notify(&self) -> bool {
        let mut notified = self.wait_cond.lock().await;

        if notified.lock_wait_count > 0 {
            notified.lock_wait_count -= 1;
        }
        return if notified.lock_wait_count == 0 {
            self.cond.notify_all();
            true
        } else {
            false
        };
    }

    pub async fn log_reset(&self) {
        let mut wait = self.wait_cond.lock().await;
        wait.log_flush = true;
    }

    pub async fn log_wait_flush(&self) -> Result<()> {
        let mut notified = self.wait_cond.lock().await;
        while notified.tx_state.is_running() && notified.log_flush {
            if notified.tx_state.is_running() && notified.tx_state != TxState::TxWaitingLogForce {
                notified.tx_state = TxState::TxWaitingLogForce;
            } else {
                panic!("error");
            }
            notified = self.cond.wait(notified).await;
        }
        if notified.tx_state == TxState::TxWaitingLogForce {
            notified.tx_state = TxState::TxIdle;
        }
        Ok(())
    }

    pub async fn log_notify_flush(&self) -> bool {
        let mut notified = self.wait_cond.lock().await;
        return if notified.log_flush {
            notified.log_flush = false;
            self.cond.notify_all();
            true
        } else {
            false
        };
    }

    pub async fn notify_abort(&self, result: Result<()>) {
        let mut notified = self.wait_cond.lock().await;
        notified.result = result;
        notified.tx_state = TxState::TxAborted;
        self.cond.notify_all();
    }

    pub async fn get_state(&self) -> TxState {
        let cond = self.wait_cond.lock().await;
        cond.tx_state
    }

    pub async fn set_state(&self, state: TxState) {
        let mut cond = self.wait_cond.lock().await;
        cond.tx_state = state;
    }
}
