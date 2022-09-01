use crate::access::page_id::PageId;
use crate::access::page_latch::PageGuardEP;
use async_trait::async_trait;
use common::result::Result;

#[async_trait]
pub trait HandlePage: Sync + Send + 'static {
    async fn handle_non_leaf(&self, parent: Option<PageId>, page: PageGuardEP) -> Result<()>;
    async fn handle_leaf(&self, parent: Option<PageId>, page: PageGuardEP) -> Result<()>;
}
