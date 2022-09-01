use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Notify};
use tokio::time::timeout;

use common::error_type::ET;
use common::result::Result;

pub struct WaitChannel<T> {
    phantom: PhantomData<T>,
}

#[derive(Clone)]
pub struct CSender<T> {
    sender: mpsc::Sender<(Option<T>, Option<Arc<Notify>>)>,
}

pub struct CReceiver<T> {
    receiver: mpsc::Receiver<(Option<T>, Option<Arc<Notify>>)>,
}

impl<T> WaitChannel<T> {
    pub fn make(n: usize) -> (CSender<T>, CReceiver<T>) {
        let (s, r) = mpsc::channel::<(Option<T>, Option<Arc<Notify>>)>(n);
        let sender = CSender::new(s);
        let receiver = CReceiver::new(r);
        (sender, receiver)
    }
}

impl<T> CSender<T> {
    fn new(sender: mpsc::Sender<(Option<T>, Option<Arc<Notify>>)>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, t: T) -> Result<()> {
        let r = self.sender.send((Some(t), None)).await;
        match r {
            Ok(()) => Ok(()),
            Err(e) => Err(ET::SenderError(e.to_string())),
        }
    }

    pub async fn close(&self) -> Result<()> {
        let r = self.sender.send((None, None)).await;
        match r {
            Ok(()) => Ok(()),
            Err(e) => Err(ET::SenderError(e.to_string())),
        }
    }

    pub async fn send_sync(&self, t: T) -> Result<Arc<Notify>> {
        //let notify = Notify::new();
        let notify = Arc::new(Notify::new());
        let result = self.sender.send((Some(t), Some(notify.clone()))).await;
        match result {
            Ok(()) => {}
            Err(e) => {
                return Err(ET::SenderError(e.to_string()));
            }
        }
        Ok(notify)
    }

    pub async fn send_sync_wait(&self, t: T) -> Result<()> {
        //let notify = Notify::new();
        let notify = Arc::new(Notify::new());
        let result = self.sender.send((Some(t), Some(notify.clone()))).await;
        match result {
            Ok(()) => {}
            Err(e) => {
                return Err(ET::SenderError(e.to_string()));
            }
        }
        notify.notified().await;
        Ok(())
    }

    pub async fn close_sync_wait(&self) -> Result<()> {
        //let notify = Notify::new();
        let notify = Arc::new(Notify::new());
        let result = self.sender.send((None, Some(notify.clone()))).await;
        match result {
            Ok(()) => {}
            Err(e) => {
                return Err(ET::SenderError(e.to_string()));
            }
        }
        notify.notified().await;
        Ok(())
    }
}

impl<T> CReceiver<T> {
    pub fn new(receiver: mpsc::Receiver<(Option<T>, Option<Arc<Notify>>)>) -> Self {
        Self { receiver }
    }

    pub fn try_recv(&mut self) -> Result<(Option<T>, Option<Arc<Notify>>)> {
        let result = self.receiver.try_recv();
        match result {
            Ok(n) => Ok(n),
            Err(e) => Err(ET::TokioRecvError(e.to_string())),
        }
    }

    pub async fn recv(&mut self) -> Result<(Option<T>, Option<Arc<Notify>>)> {
        let result = self.receiver.recv().await;
        match result {
            Some(n) => Ok(n),
            None => Err(ET::EOF),
        }
    }

    pub async fn recv_timeout(&mut self, millis: u64) -> Result<(Option<T>, Option<Arc<Notify>>)> {
        let result = timeout(Duration::from_millis(millis), self.receiver.recv()).await;

        return match result {
            Ok(opt) => match opt {
                Some(n) => Ok(n),
                None => Err(ET::EOF),
            },
            Err(_elapsed) => Err(ET::TokioRecvError("timeout".to_string())),
        };
    }
}

#[cfg_attr(debug_assertions, inline(never))]
fn stop_or_notify<T>(
    vec_notify: &mut Vec<Arc<Notify>>,
    opt_n: Option<T>,
    opt_notify: Option<Arc<Notify>>,
) -> Result<()> {
    if opt_n.is_none() {
        // stop
        match opt_notify {
            Some(notify) => {
                notify.notify_one();
            }
            None => {}
        }
        return Err(ET::EOF);
    }
    match opt_notify {
        Some(n) => vec_notify.push(n),
        None => {}
    }
    Ok(())
}

#[cfg_attr(debug_assertions, inline(never))]
pub async fn wait_notify_or_timeout<T>(
    vec_notify: &mut Vec<Arc<Notify>>,
    receiver: &mut CReceiver<T>,
    millis: u64,
) -> Result<()> {
    if millis == 0 {
        return Ok(());
    }
    let (opt1, opt2) = receiver.recv_timeout(millis).await?;
    stop_or_notify(vec_notify, opt1, opt2)?;
    loop {
        let result = receiver.try_recv();
        match result {
            Ok((opt1, opt2)) => {
                stop_or_notify(vec_notify, opt1, opt2)?;
            }
            Err(_) => {
                break;
            }
        }
    }
    Ok(())
}
