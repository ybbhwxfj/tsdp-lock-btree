/// clog record the command log
/// currently the command includes:
///     write(include update/insert/delete) a key value pair
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error};
use tokio::sync::{Mutex, Notify};
use walkdir::WalkDir;

use common::error_type::ET;
use common::id::OID;
use common::result::{io_result, Result};

use crate::access::clog::{CLog, CLogRecType, CmdCheckpoint, CmdTxOp, CmdUpdateKV, LSN};
use crate::access::clog_rec::{CLogRec, CUpdateKV};
use crate::access::clog_replay::CLogReplay;
use crate::access::const_val::FILE_EXT_CLOG;
use crate::access::file::{async_flush, async_open_file, async_read_all, async_write_all};
use crate::access::wait_notify::{CReceiver, CSender, WaitChannel};

pub const LOG_BUFFER_SIZE: u32 = 1024 * 1024;

pub struct CLogImpl {
    base_path: String,
    log_flush_millis: u64,
    checkpoint_millis: u64,
    log_buffer_size: u64,
    log_buffered: Arc<Mutex<LogBuffered>>,
    init_done: Arc<Notify>,
    log_flush: Arc<Mutex<LogFlush>>,
    // notify the buffer pool to flush all pages
    checkpoint_notify: CSender<()>,
    flush_notify_sender: CSender<Vec<u8>>,
}

struct LogBuffered {
    log_buffer_size: u64,
    buffer: Vec<u8>,
    lsn: u32,
}

struct LogFlush {
    base_path: String,
    prev_log_file_id: u32,
    prev_log_file_size: u32,
    max_lsn: u32,
    flush_notify_receiver: CReceiver<Vec<u8>>,
}

impl CLogImpl {
    pub fn new(
        base_path: String,
        log_flush_millis: u64,
        checkpoint_millis: u64,
        log_buffer_size: u64,
        log_queue_size: u64,
        pool_notifier: CSender<()>,
    ) -> Self {
        let (log_flush_s, log_flush_r) = WaitChannel::make(log_queue_size as usize);
        CLogImpl {
            base_path: base_path.clone(),
            log_flush_millis,
            checkpoint_millis,
            log_buffer_size,
            log_buffered: Arc::new(Mutex::new(LogBuffered::new(log_buffer_size))),
            init_done: Arc::new(Notify::new()),
            log_flush: Arc::new(Mutex::new(LogFlush::new(base_path.clone(), log_flush_r))),
            checkpoint_notify: pool_notifier,
            flush_notify_sender: log_flush_s,
        }
    }

    pub async fn open<F: CLogReplay>(&self, f: &F) -> Result<()> {
        if !Path::new(&self.base_path).exists() {
            let r = tokio::fs::create_dir_all(self.base_path.clone()).await;
            io_result(r)?;
        }
        let r = self.recovery(f).await;
        self.init_done.notify_one();
        return r;
    }

    pub async fn close(&self) -> Result<()> {
        self.log_flush(true).await?;
        self.close_channel().await?;
        Ok(())
    }

    pub async fn loop_flush(&self) -> Result<()> {
        self.init_done.notified().await;
        let mut guard = self.log_flush.lock().await;
        guard.flush_loop().await
    }

    pub async fn close_channel(&self) -> Result<()> {
        let r1 = self.flush_notify_sender.close_sync_wait().await;
        let r2 = self.checkpoint_notify.close_sync_wait().await;
        r1?;
        r2
    }

    async fn append(
        &self,
        cmd: u32,
        table_id: OID,
        tuple_id: OID,
        key: &[u8],
        value: &[u8],
    ) -> Result<u32> {
        let mut log_buffered = self.log_buffered.lock().await;
        let lsn = log_buffered.append_log(cmd, table_id, tuple_id, key, value)?;
        Ok(lsn)
    }

    async fn recovery<F>(&self, replay: &F) -> Result<()>
    where
        F: CLogReplay,
    {
        let mut prev_id = 0;
        let mut prev_size = 0;
        let mut lsn = 0;
        let ids = self.walk_log_directory()?;

        let mut buffer = Vec::with_capacity(self.log_buffer_size as usize);
        for id in ids {
            let mut path = PathBuf::from(self.base_path.clone());
            path = path.join(format!("{}.{}", id.to_string(), FILE_EXT_CLOG));
            let mut file = async_open_file(path).await?;
            let r = file.metadata().await;
            let meta = io_result(r)?;
            let file_size = meta.len();
            if file_size > self.log_buffer_size as u64 {
                panic!("ERROR");
            }
            buffer.resize(file_size as usize, 0);
            async_read_all(&mut file, buffer.as_mut_slice()).await?;
            self.recovery_buffer(&buffer, &mut lsn, replay).await?;
            prev_id = id;
            prev_size = file_size as u32;
        }
        {
            let mut log_buffered = self.log_buffered.lock().await;
            log_buffered.init(lsn);

            let mut guard_log_flush = self.log_flush.lock().await;
            guard_log_flush.init(prev_id, prev_size, lsn);
        }

        Ok(())
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.checkpoint_notify.send_sync_wait(()).await
    }

    async fn log_flush(&self, sync: bool) -> Result<()> {
        let mut buffer = self.log_buffered.lock().await;
        let buffer = buffer.buffer_move();
        let vec = match buffer {
            Some(vec) => vec,
            None => {
                return Ok(());
            }
        };
        if sync {
            let notify = self.flush_notify_sender.send_sync(vec).await?;
            notify.notified().await;
        } else {
            self.flush_notify_sender.send(vec).await?;
        }
        Ok(())
    }

    async fn recovery_buffer<F>(&self, buf: &Vec<u8>, max_lsn: &mut u32, replay: &F) -> Result<()>
    where
        F: CLogReplay,
    {
        let mut offset = 0;
        while offset < buf.len() {
            let r = CLogRec::load(&buf[offset..])?;
            let lsn = r.lsn();
            let clog_type = r.clog_type();
            let size = r.size() as usize;

            if *max_lsn < lsn {
                *max_lsn = lsn;
            }
            let begin = offset + CLogRec::header_size();
            let end = offset + size;
            if buf.len() >= begin && buf.len() >= end {
                match clog_type {
                    CLogRecType::CUpdateKV => {
                        replay.replay(lsn, &buf[begin..end]).await?;
                    }
                    _ => {}
                }
            } else {
                panic!("error size")
            }

            offset += size;
        }
        Ok(())
    }

    fn walk_log_directory(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        for entry in WalkDir::new(self.base_path.clone()) {
            let dir_entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    return Err(ET::IOError(e.to_string()));
                }
            };
            match dir_entry.path().extension() {
                Some(s) => {
                    if !s.eq(OsStr::new(FILE_EXT_CLOG)) {
                        continue;
                    }
                }
                None => {
                    continue;
                }
            }
            match dir_entry.metadata() {
                Ok(meta) => {
                    if !meta.is_file() {
                        continue;
                    }
                }
                Err(_e) => {
                    continue;
                }
            }
            let stem = match dir_entry.path().file_stem() {
                Some(s) => match s.to_str() {
                    Some(ss) => ss.to_string(),
                    None => {
                        continue;
                    }
                },
                None => {
                    continue;
                }
            };
            let id = match u32::from_str_radix(stem.as_str(), 10) {
                Ok(i) => i,
                Err(_) => {
                    continue;
                }
            };
            ids.push(id);
        }
        ids.sort();
        Ok(ids)
    }
}

impl LogBuffered {
    pub fn new(log_buffer_size: u64) -> Self {
        LogBuffered {
            log_buffer_size,
            buffer: vec![],
            lsn: 0,
        }
    }

    pub fn init(&mut self, lsn: u32) {
        self.lsn = lsn
    }

    fn append_log(
        &mut self,
        cmd: u32,
        table_id: OID,
        oid: OID,
        key: &[u8],
        value: &[u8],
    ) -> Result<u32> {
        let opt_lsn = self.append_log_inner(cmd, table_id, oid, key, value)?;
        match opt_lsn {
            Some(lsn) => Ok(lsn),
            None => {
                self.buffer_move();
                let opt_lsn1 = self.append_log_inner(cmd, table_id, oid, key, value)?;
                match opt_lsn1 {
                    Some(lsn1) => Ok(lsn1),
                    None => {
                        panic!("ERROR");
                    }
                }
            }
        }
    }

    fn buffer_move(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            return None;
        }
        let vec = if self.buffer.len() * 3 / 2 < self.log_buffer_size as usize {
            let vec = Vec::from(self.buffer.as_slice());
            self.buffer.resize(0, 0);
            vec
        } else {
            let mut vec = Vec::new();
            vec.reserve(self.log_buffer_size as usize);
            std::mem::swap(&mut vec, &mut self.buffer);
            vec
        };
        return Some(vec);
    }

    fn append_log_inner(
        &mut self,
        cmd: u32,
        table_id: OID,
        tuple_id: OID,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<u32>> {
        let size = CLogRec::header_size() + CUpdateKV::header_size() + key.len() + value.len();
        return if self.buffer.len() + size < self.log_buffer_size as usize {
            let begin = self.buffer.len();
            self.buffer.resize(begin + size, 0);
            self.lsn += 1;
            let lsn = self.lsn;
            CLogRec::format_up_kv_cmd(
                &mut self.buffer[begin..],
                cmd,
                lsn,
                table_id,
                tuple_id,
                key,
                value,
            );
            Ok(Some(lsn))
        } else {
            Ok(None)
        };
    }
}

impl LogFlush {
    pub fn new(base_path: String, receiver: CReceiver<Vec<u8>>) -> Self {
        LogFlush {
            base_path,
            prev_log_file_id: 0,
            prev_log_file_size: 0,
            max_lsn: 0,
            flush_notify_receiver: receiver,
        }
    }

    fn init(&mut self, prev_log_file_id: u32, prev_log_file_size: u32, max_lsn: u32) {
        self.prev_log_file_size = prev_log_file_size;
        self.prev_log_file_id = prev_log_file_id;
        self.max_lsn = max_lsn;
    }

    async fn flush_loop(&mut self) -> Result<()> {
        loop {
            let (opt1, opt2) = self.flush_notify_receiver.recv().await?;
            let (result, close) = match opt1 {
                Some(vec) => {
                    let r = self.flush_buffer(vec).await;
                    (r, false)
                }
                None => (Ok(()), true),
            };
            match opt2 {
                Some(notify) => {
                    notify.notify_one();
                }
                None => {}
            }
            if close {
                break;
            }
            match &result {
                Ok(()) => {}
                Err(e) => {
                    error!("{}", e.to_string());
                    return result;
                }
            }
        }
        Ok(())
    }

    async fn flush_buffer(&mut self, log: Vec<u8>) -> Result<()> {
        let id = if self.prev_log_file_size as usize + log.len() < LOG_BUFFER_SIZE as usize {
            // write to previous log file
            let id = self.prev_log_file_id;
            id
        } else {
            // write to a new file
            self.prev_log_file_id += 1;
            let id = self.prev_log_file_id;
            id
        };
        let mut path = PathBuf::from(self.base_path.clone());
        path = path.join(format!("{}.{}", id.to_string(), FILE_EXT_CLOG));
        debug!("open {}", path.as_os_str().to_str().unwrap());
        let mut file = async_open_file(path).await?;
        async_write_all(&mut file, log.as_slice()).await?;
        async_flush(&mut file).await?;
        Ok(())
    }
}

impl Clone for CLogImpl {
    fn clone(&self) -> Self {
        Self {
            base_path: self.base_path.clone(),
            log_flush_millis: self.log_flush_millis,
            checkpoint_millis: self.checkpoint_millis,
            log_buffer_size: self.log_buffer_size,
            log_buffered: self.log_buffered.clone(),
            init_done: self.init_done.clone(),
            log_flush: self.log_flush.clone(),
            checkpoint_notify: self.checkpoint_notify.clone(),
            flush_notify_sender: self.flush_notify_sender.clone(),
        }
    }
}

#[async_trait]
impl CLog for CLogImpl {
    async fn append_kv_update(
        &self,
        cmd: CmdUpdateKV,
        _xid: OID,
        table_id: OID,
        tuple_id: OID,
        key: &[u8],
        value: &[u8],
    ) -> Result<LSN> {
        self.append(cmd as u32, table_id, tuple_id, key, value)
            .await
    }

    async fn append_tx_op(&self, _cmd: CmdTxOp, _xid: OID) -> Result<LSN> {
        // todo!()
        Ok(0)
    }

    async fn append_checkpoint(&self, _cmd: CmdCheckpoint, _checkpoint_id: OID) -> Result<LSN> {
        // todo!()
        Ok(0)
    }

    async fn flush(&self, sync: bool) -> Result<()> {
        self.log_flush(sync).await?;
        Ok(())
    }
}
