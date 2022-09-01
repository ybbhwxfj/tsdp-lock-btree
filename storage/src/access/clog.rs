use async_trait::async_trait;

use common::id::OID;
use common::result::Result;

pub type LSN = u32;

const CMD_UPDATE_KEY_VALUE: u32 = 1;
const CMD_CHECKPOINT: u32 = 2;

pub enum CLogRecType {
    CUpdateKV = CMD_UPDATE_KEY_VALUE as isize,
    CCheckpoint = CMD_CHECKPOINT as isize,
}

pub enum CmdCheckpoint {
    CPBegin = 1,
    CPEnd = 2,
}

const UP_CMD_KV_WRITE: u32 = 1;
const UP_CMD_KV_DELETE: u32 = 2;

pub enum CmdUpdateKV {
    KVWrite = UP_CMD_KV_WRITE as isize,
    KVDelete = UP_CMD_KV_DELETE as isize,
}

pub enum CmdTxOp {
    TxBegin = 1,
    TxCommit = 2,
    TxAbort = 3,
}

#[async_trait]
pub trait CLog: Send + Sync + Clone {
    async fn append_kv_update(
        &self,
        cmd: CmdUpdateKV,
        xid: OID,
        table_id: OID,
        oid: OID,
        key: &[u8],
        value: &[u8],
    ) -> Result<LSN>;

    async fn append_tx_op(&self, cmd: CmdTxOp, xid: OID) -> Result<LSN>;

    async fn append_checkpoint(&self, cmd: CmdCheckpoint, checkpoint_id: OID) -> Result<LSN>;

    async fn flush(&self, sync: bool) -> Result<()>;
}
