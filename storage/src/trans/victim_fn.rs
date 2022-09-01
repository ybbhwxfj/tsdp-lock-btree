use async_trait::async_trait;

use common::id::XID;

#[async_trait]
pub trait VictimFn: Send + Sync {
    async fn victim(&self, xid: XID);
}
