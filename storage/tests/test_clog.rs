use std::mem::size_of;

use async_trait::async_trait;
use log::{debug, error, trace};
use tempfile::tempdir;
use tokio::runtime::Builder;

use common::error_type::ET;
use common::id::{gen_oid, gen_xid};
use common::result::Result;
use storage::access::clog::{CLog, CmdTxOp, CmdUpdateKV, LSN};
use storage::access::clog_impl::CLogImpl;
use storage::access::clog_rec::CUpdateKV;
use storage::access::clog_replay::CLogReplay;
use storage::access::get_set_int::{get_u32, set_u32};
use storage::access::wait_notify::{CReceiver, CSender, WaitChannel};

pub mod test_storage;

struct LogReplayStub {}

impl LogReplayStub {
    pub fn new() -> Self {
        Self {}
    }
}

impl Clone for LogReplayStub {
    fn clone(&self) -> Self {
        todo!()
    }
}

#[async_trait]
impl CLogReplay for LogReplayStub {
    async fn replay(&self, _lsn: LSN, log_rec: &[u8]) -> Result<()> {
        let rec = CUpdateKV::from(log_rec);
        let table_id = rec.table_id();
        let tuple_id = rec.tuple_id();
        let key = rec.key();
        let value = rec.value();
        let ik = slice_to_u32(key)?;
        let iv = slice_to_u32(value)?;
        debug!(
            "replay log, table : [{}], tuple : [{}], key : [{}], value : [{}]",
            table_id, tuple_id, ik, iv
        );
        Ok(())
    }
}

async fn task_clog_flush(clog: CLogImpl) -> Result<()> {
    let r = clog.loop_flush().await;
    match &r {
        Ok(()) => {}
        Err(e) => {
            error!("{}", e.to_string());
        }
    }
    return r;
}

async fn task_pool_flush_stub(receiver: CReceiver<()>) -> Result<()> {
    let mut recv = receiver;
    loop {
        let (opt1, opt2) = recv.recv().await?;
        let close = opt1.is_none();
        match opt2 {
            Some(notify) => notify.notify_one(),
            None => {
                return Err(ET::EOF);
            }
        }
        if close {
            return Ok(());
        }
    }
}

async fn task_clog_append(clog: CLogImpl) -> Result<()> {
    let r = clog_append_test(clog).await;
    match &r {
        Ok(()) => {}
        Err(e) => {
            error!("{}", e.to_string())
        }
    }
    r
}

async fn clog_append_test(clog: CLogImpl) -> Result<()> {
    let stub = LogReplayStub::new();
    let r_open = clog.open(&stub).await;
    match &r_open {
        Ok(()) => {}
        Err(_) => {
            let _ = clog.close_channel().await;
            return r_open;
        }
    }
    for i in 0..1000 {
        let xid = gen_xid();
        let table_id = gen_oid();
        let tuple_id = gen_oid();
        let key = u32_to_vec(i);
        let value = u32_to_vec(i);
        trace!(
            "append log, table : [{}], tuple : [{}], key : [{}], value : [{}]",
            table_id,
            tuple_id,
            i,
            i
        );
        clog.append_tx_op(CmdTxOp::TxBegin, xid).await?;
        clog.append_kv_update(
            CmdUpdateKV::KVWrite,
            xid,
            table_id,
            tuple_id,
            key.as_slice(),
            value.as_slice(),
        )
        .await?;
        clog.append_tx_op(CmdTxOp::TxCommit, xid).await?;
    }

    clog.flush(true).await?;
    clog.close().await?;
    Ok(())
}

fn create_clog(pool_notifier: CSender<()>, base_path: String) -> Result<CLogImpl> {
    let clog = CLogImpl::new(base_path, 1000, 1000, 1024 * 1024, 1024, pool_notifier);

    Ok(clog)
}

fn test_run(base_path: String) -> Result<()> {
    let (sender, receiver) = WaitChannel::make(10);
    let clog = create_clog(sender, base_path)?;
    let f1 = task_clog_flush(clog.clone());
    let f2 = task_clog_append(clog.clone());
    let f3 = task_pool_flush_stub(receiver);
    let local = tokio::task::LocalSet::new();
    local.spawn_local(f1);
    local.spawn_local(f2);
    local.spawn_local(f3);
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(local);
    Ok(())
}

#[test]
fn test_clog() {
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

    let r1 = test_run(base_path.clone());
    let _r2 = test_run(base_path.clone());
    match r1 {
        Ok(()) => {}
        Err(e) => {
            error!("{}", e.to_string());
            assert!(false);
        }
    }
}

fn u32_to_vec(key: u32) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.resize(size_of::<u32>(), 0);
    set_u32(vec.as_ptr(), vec.len(), 0, key);
    return vec;
}

fn slice_to_u32(slice: &[u8]) -> Result<u32> {
    if slice.len() >= size_of::<u32>() {
        Ok(get_u32(slice.as_ptr(), slice.len(), 0))
    } else {
        Err(ET::FatalError("length error".to_string()))
    }
}
