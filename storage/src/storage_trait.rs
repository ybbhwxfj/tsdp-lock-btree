use async_trait::async_trait;

use adt::range::RangeBounds;
use adt::slice::Slice;
use common::id::OID;
use common::result::Result;

use crate::access::access_opt::{DeleteOption, InsertOption, SearchOption, UpdateOption};
use crate::access::handle_read::HandleReadKV;
use crate::access::handle_write::HandleWrite;

use crate::trans::tx_context::TxContext;

#[async_trait]
trait StorageTrait {
    async fn open_table(&self, table: &String) -> Result<OID>;

    async fn close_table(&self, oid: OID);

    async fn search_key<K, F>(
        &self,
        context: &TxContext,
        oid: OID,
        key: &K,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>;

    /// range search
    async fn search_range<K, F>(
        &self,
        context: &TxContext,
        table_oid: OID,
        range: &RangeBounds<K>,
        opt: SearchOption,
        handler: &F,
    ) -> Result<()>
    where
        K: Slice,
        F: HandleReadKV;

    async fn insert<K, V, H>(
        &self,
        context: &mut TxContext,
        table_oid: OID,
        key: &K,
        value: &V,
        opt: InsertOption,
        handler: &H,
    ) -> Result<()>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite;

    /// update a key/value pair
    async fn update<K, V, H>(
        &self,
        context: &TxContext,
        table_oid: OID,
        key: &K,
        value: &V,
        opt: UpdateOption,
        handler: &H,
    ) -> Result<()>
    where
        K: Slice,
        V: Slice,
        H: HandleWrite;

    /// update a key/value pair
    async fn delete<K, HW>(
        &self,
        context: &TxContext,
        table_oid: OID,
        key: &K,
        opt: DeleteOption,
        handler: &HW,
    ) -> Result<()>
    where
        K: Slice,
        HW: HandleWrite;

    async fn commit_tx(&self, context: &TxContext) -> Result<()>;

    async fn abort_tx(&self, context: &TxContext) -> Result<()>;
}
