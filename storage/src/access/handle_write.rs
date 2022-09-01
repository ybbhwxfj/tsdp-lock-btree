use async_trait::async_trait;

use common::result::Result;

#[async_trait]
pub trait HandleWrite: Send + Sync + Clone {
    /// insert_value : update value passing by parameter
    /// return : option value
    ///     if the insert value to the index is exactly insert_value, then return None
    ///     else construct the value insert to the index, used by non-cluster index to
    async fn on_insert(&self, insert_value: &[u8]) -> Result<Option<Vec<u8>>>;

    /// update_value : update value passing by parameter
    /// ref_value: reference to a slice in page
    /// return : option value
    ///     if the insert value to the index is exactly insert_value, then return None
    ///     else construct the value insert to the index, used by non-cluster index to
    async fn on_update(&self, update_value: &[u8], ref_value: &[u8]) -> Result<Option<Vec<u8>>>;

    /// ref_value: reference to a slice in page
    /// return : result
    async fn on_delete(&self, ref_value: &[u8]) -> Result<()>;
}

pub struct EmptyHandleWrite {}

impl EmptyHandleWrite {
    pub fn new() -> Self {
        EmptyHandleWrite {}
    }
}

impl Default for EmptyHandleWrite {
    fn default() -> Self {
        EmptyHandleWrite {}
    }
}

#[async_trait]
impl HandleWrite for EmptyHandleWrite {
    async fn on_insert(&self, _insert_value: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn on_update(&self, _update_value: &[u8], _ref_value: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn on_delete(&self, _ref_value: &[u8]) -> Result<()> {
        Ok(())
    }
}

unsafe impl Send for EmptyHandleWrite {}

unsafe impl Sync for EmptyHandleWrite {}

impl Clone for EmptyHandleWrite {
    fn clone(&self) -> Self {
        Self::new()
    }
}
