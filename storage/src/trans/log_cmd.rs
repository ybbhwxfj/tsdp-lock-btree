use serde::{Deserialize, Serialize};

use common::id::XID;

use crate::trans::write_operation::WriteOp;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogCmd {
    LogBeginTx(XID),
    LogCommitTx(XID),
    LogAbortTx(XID),
    LogWriteOp(XID, WriteOp),
    // stop label, not a real log command
    LogStop,
}

unsafe impl Send for LogCmd {}
