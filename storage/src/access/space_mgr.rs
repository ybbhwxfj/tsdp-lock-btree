use std::sync::Arc;

use roaring::RoaringBitmap;
use tokio::sync::Mutex;

use crate::access::page_id::PageId;

#[derive(Clone)]
pub struct SpaceMgr {
    bitmap: Arc<Mutex<RoaringBitmap>>,
    page_num: Arc<Mutex<u32>>,
}

impl SpaceMgr {
    pub fn new() -> Self {
        SpaceMgr {
            bitmap: Arc::new(Mutex::new(RoaringBitmap::new())),
            page_num: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn max_page_id(&self) -> PageId {
        let guard = self.page_num.lock().await;
        return *guard - 1;
    }

    pub async fn new_page_id(&self) -> PageId {
        let mut bitmap = self.bitmap.lock().await;
        match bitmap.max() {
            Some(id) => {
                bitmap.remove(id);
                id
            }
            None => {
                let mut page_num = self.page_num.lock().await;
                let id = *page_num;
                *page_num += 1;
                id
            }
        }
    }

    pub async fn set_page_num(&self, num: u32) {
        let mut page_num = self.page_num.lock().await;
        *page_num = num;
    }

    pub async fn page_removed(&self, page_id: PageId) {
        let mut bitmap = self.bitmap.lock().await;
        bitmap.insert(page_id);
    }
}
