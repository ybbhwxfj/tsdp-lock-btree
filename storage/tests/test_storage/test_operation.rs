use std::ops::Bound;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use common::id::{OID, XID};
use storage::access::access_opt::{DeleteOption, InsertOption, SearchOption, UpdateOption};

#[derive(Clone, Serialize, Deserialize)]
pub enum TestTxOpEnum<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    TTOBegin(XID),
    TTOCommit(XID),
    TTOAbort(XID),
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    TTOAccess(XID, OID, TestOpEnum<K, V>), // tx id, table id, operation
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TestOpEnum<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    Insert(TestOpInsert<K, V>),
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    Update(TestOpUpdate<K, V>),
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    Delete(TestOpDelete<K>),
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    SearchKey(TestOpSearchKey<K, V>),
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    SearchRange(TestOpSearchRange<K, V>),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestOpInsert<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub key: K,
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub value: V,
    pub option: InsertOption,
    pub result: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestOpUpdate<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub key: K,
    #[serde(bound = "V: Ord + Clone + DeserializeOwned + Serialize")]
    pub value: V,
    pub option: UpdateOption,
    pub result: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestOpDelete<K: Ord + Clone + DeserializeOwned + Serialize> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub key: K,
    pub option: DeleteOption,
    pub result: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestOpSearchKey<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub key: K,
    //#[serde(bound = "V: Ord + Clone + DeserializeOwned + Serialize")]
    //value: V,
    pub option: SearchOption,
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub result: Option<Vec<(K, V)>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestOpSearchRange<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub low: Bound<K>,
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize")]
    pub up: Bound<K>,
    pub option: SearchOption,
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub result: Option<Vec<(K, V)>>,
}
