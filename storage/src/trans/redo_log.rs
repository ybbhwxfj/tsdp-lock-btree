use std::sync::Arc;

use async_channel::{Receiver, Sender};
use async_std::fs::{File, OpenOptions};
use async_std::io::WriteExt;
use async_std::path::Path;
use async_std::sync::Mutex;
use tracing::{debug, trace_span};
use tracing_futures::Instrument;

use common::result::io_result;
use common::result::Result;

use crate::trans::log_cmd::LogCmd;
use crate::trans::tx_wait_notifier::TxWaitNotifier;

const MAX_LOG_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 8; // 8MB

pub struct RedoLog {
    path: String,
    file: Mutex<Option<File>>,
    file_no: Mutex<u64>,
    sender_ch: Sender<(Option<Arc<TxWaitNotifier>>, LogCmd)>,
    receiver_ch: Receiver<(Option<Arc<TxWaitNotifier>>, LogCmd)>,
}

impl RedoLog {
    pub fn new(path: String) -> Self {
        let (s, r) = async_channel::bounded(10000);

        Self {
            path,
            file: Mutex::new(None),
            file_no: Default::default(),
            sender_ch: s,
            receiver_ch: r,
        }
    }

    pub async fn append_log(&self, log: LogCmd, notifier: Option<Arc<TxWaitNotifier>>) {
        debug!("append log {}", serde_json::to_string(&log).unwrap());
        let _ = self
            .sender_ch
            .send((notifier, log))
            .instrument(trace_span!("append log send"))
            .await;
        debug!("append log done");
    }

    /// invoked by one threads
    pub async fn flushing_log(&self) -> Result<()> {
        let mut vec = Vec::new();
        let cap = 1024 * 1024; // 1KB buffer;
        vec.reserve(cap);
        loop {
            let opt = self.receiver_ch.recv().await;
            match opt {
                Ok((opt_notify, log)) => {
                    match log {
                        LogCmd::LogStop => {
                            break;
                        }
                        _ => {}
                    }
                    let result = bincode::serialize_into(&mut vec, &log);
                    match result {
                        Ok(()) => {}
                        Err(_) => {
                            self.write_log(vec.as_slice(), true).await?;
                            unsafe {
                                vec.set_len(0);
                            }
                            let r_s = bincode::serialize_into(&mut vec, &log);
                            match r_s {
                                Ok(()) => {}
                                Err(_) => {
                                    panic!("serialize error");
                                }
                            }
                        }
                    }
                    match opt_notify {
                        Some(n) => {
                            self.write_log(vec.as_slice(), true).await?;
                            unsafe {
                                vec.set_len(0);
                            }
                            // TODO notify tx
                            let _ = n.log_notify_flush().await;
                        }
                        None => {
                            if vec.len() > cap {
                                self.write_log(vec.as_slice(), true).await?;
                                unsafe {
                                    vec.set_len(0);
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn stop(&self) {
        let _ = self.sender_ch.send((None, LogCmd::LogStop)).await;
    }

    async fn write_log(&self, buf: &[u8], sync: bool) -> Result<()> {
        let mut opt_file = self.file.lock().await;

        let file_size = {
            match &mut (*opt_file) {
                Some(file) => {
                    let r1 = file.metadata().await;
                    let meta = io_result(r1)?;
                    meta.len()
                }
                None => {
                    let file = self.create_log_file().await?;
                    *opt_file = Some(file.clone());
                    let r1 = file.metadata().await;
                    let meta = io_result(r1)?;
                    meta.len()
                }
            }
        };
        if file_size + buf.len() as u64 > MAX_LOG_FILE_SIZE {
            {
                let file = match &mut (*opt_file) {
                    Some(f) => f,
                    None => {
                        panic!("error");
                    }
                };
                let r = file.sync_all().await;
                io_result(r)?;
            }

            {
                let file = self.create_log_file().await?;
                *opt_file = Some(file);
            }
        };
        {
            let file = match &mut (*opt_file) {
                Some(f) => f,
                None => {
                    panic!("error");
                }
            };
            let r2 = file.write_all(buf).await;
            io_result(r2)?;

            if sync {
                let r3 = file.sync_all().await;
                io_result(r3)?;
            }
        }
        Ok(())
    }

    async fn create_log_file(&self) -> Result<File> {
        let next_log_id = {
            let mut no = self.file_no.lock().await;
            *no = *no + 1;
            *no
        };
        let log_file = next_log_id.to_string() + ".log";
        let path = Path::new(self.path.as_str()).join(log_file);

        let open_r = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path)
            .instrument(trace_span!("File::open"))
            .await;
        io_result(open_r)
    }
}
