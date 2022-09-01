use serde::Serialize;

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize)]
pub enum TxState {
    TxIdle,
    TxWaitingLogForce,
    TxWaitingLock,
    TxCommitted,
    TxAborted,
}

impl TxState {
    pub fn is_running(&self) -> bool {
        match self {
            TxState::TxCommitted => false,
            TxState::TxAborted => false,
            _ => true,
        }
    }
}
