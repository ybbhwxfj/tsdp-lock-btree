use std::collections::HashSet;
use std::sync::Arc;

use log::error;
use tempfile::tempdir;
use tokio::runtime::Builder;

use common::id::gen_oid;
use common::result::Result;
use storage::access::buf_pool::BufPool;
use storage::access::const_val::{FileKind, INVALID_PAGE_ID};
use storage::access::latch_type::PLatchType;
use storage::access::page_hdr::PageHdrMut;
use storage::access::page_id::{PageId, PageIndex};
use storage::access::predicate::EmptyPredTable;

pub mod test_storage;

async fn test_buf_pool_run(pool: Arc<BufPool>) {
    let r = test_buf_pool_run_inner(pool).await;
    match r {
        Ok(()) => {}
        Err(e) => {
            error!("{}", e.to_string())
        }
    }
}

async fn test_buf_pool_run_inner(pool: Arc<BufPool>) -> Result<()> {
    let mut hash_set = HashSet::new();
    let page_size = pool.page_size() as usize;
    for _i in 0..10 {
        let file_id = gen_oid();
        for j in 0..100 {
            let mut vec = Vec::with_capacity(page_size);
            vec.resize(page_size as usize, 0);
            let mut page = PageHdrMut::from(&mut vec);
            page.format(1, INVALID_PAGE_ID, INVALID_PAGE_ID, FileKind::FileHeap);
            let page_id = j as PageId;
            let index = PageIndex::new(file_id, page_id);
            hash_set.insert(index);
            pool.add_page::<EmptyPredTable>(index, vec).await?;
            pool.add_dirty(index).await?;
        }
    }
    pool.checkpoint().await?;

    for index in hash_set {
        let latch = pool.get_hp_page(&index).await?;
        let guard = latch.lock(PLatchType::ReadLock, gen_oid()).await;
        guard.unlock().await;
    }

    pool.close().await;
    Ok(())
}

#[test]
fn test_pool() {
    let buff_size = 1024 * 1024; // 1MiB
    let page_size = 1024;
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
    let path = match tempdir() {
        Ok(p) => p,
        Err(e) => {
            panic!("ERROR:{}", e.to_string());
        }
    };
    let base_path = path.path().to_str().unwrap().to_string();
    let pool = Arc::new(BufPool::new(buff_size, page_size, base_path));

    let local = tokio::task::LocalSet::new();

    let pool_loop_flush = pool.clone();
    let pool_collecting_dirty = pool.clone();
    local.spawn_local(async move {
        let ok = pool_loop_flush.loop_flush().await;
        assert!(ok.is_ok());
    });
    local.spawn_local(async move {
        let ok = pool_collecting_dirty.collecting_dirty_page_id().await;
        assert!(ok.is_ok());
    });

    local.spawn_local(async move {
        let _ = test_buf_pool_run(pool).await;
    });
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(local);
}
