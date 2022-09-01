/// trait replay command log
use async_trait::async_trait;

use common::result::Result;

use crate::access::clog::LSN;

#[async_trait]
pub trait CLogReplay: Send + Sync + Clone {
    async fn replay(&self, lsn: LSN, log_rec: &[u8]) -> Result<()>;
}

struct EmptyCLogReplay {}

impl Clone for EmptyCLogReplay {
    fn clone(&self) -> Self {
        Self {}
    }
}

#[async_trait]
impl CLogReplay for EmptyCLogReplay {
    async fn replay(&self, _lsn: LSN, _log_record: &[u8]) -> Result<()> {
        Ok(())
    }
}
