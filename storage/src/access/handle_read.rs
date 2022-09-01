use async_trait::async_trait;

use common::id::OID;
use common::result::Result;

#[async_trait]
pub trait HandleReadKV: Send + Sync + Clone {
    async fn on_read(&self, key: &[u8], value: &[u8]) -> Result<()>;
}

#[async_trait]
pub trait HandleReadTuple: Send + Sync + Clone {
    async fn on_read(&self, oid: OID, tuple: &[u8]) -> Result<()>;
}

pub struct EmptyHandleReadKV {}

pub struct EmptyHandleReadTuple {}

impl EmptyHandleReadKV {
    pub fn new() -> Self {
        EmptyHandleReadKV {}
    }
}

unsafe impl Send for EmptyHandleReadKV {}

unsafe impl Sync for EmptyHandleReadKV {}

impl Clone for EmptyHandleReadKV {
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[async_trait]
impl HandleReadKV for EmptyHandleReadKV {
    async fn on_read(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        Ok(())
    }
}

impl EmptyHandleReadTuple {
    pub fn new() -> Self {
        EmptyHandleReadTuple {}
    }
}

unsafe impl Send for EmptyHandleReadTuple {}

unsafe impl Sync for EmptyHandleReadTuple {}

impl Clone for EmptyHandleReadTuple {
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[async_trait]
impl HandleReadTuple for EmptyHandleReadTuple {
    async fn on_read(&self, _oid: OID, _tuple: &[u8]) -> Result<()> {
        Ok(())
    }
}
