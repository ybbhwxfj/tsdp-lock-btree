use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::test_storage::test_operation::{TestOpEnum, TestTxOpEnum};
use common::id::{OID, XID};

#[derive(Clone, Serialize, Deserialize)]
pub struct TestCaseNonTx<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub init: Vec<TestOpEnum<K, V>>,
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub test: Vec<TestOpEnum<K, V>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TestCaseTx<
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
> {
    // table id , insert key values
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub init: HashMap<OID, Vec<TestOpEnum<K, V>>>,

    // transaction id, transaction operation
    #[serde(bound = "K: Ord + Clone + DeserializeOwned + Serialize, \
        V:Ord + Clone + DeserializeOwned + Serialize")]
    pub test: HashMap<XID, Vec<TestTxOpEnum<K, V>>>,
}

impl<
        K: Ord + Clone + DeserializeOwned + Serialize,
        V: Ord + Clone + DeserializeOwned + Serialize,
    > TestCaseNonTx<K, V>
{
    #[allow(dead_code)]
    pub fn new(init: Vec<TestOpEnum<K, V>>, test: Vec<TestOpEnum<K, V>>) -> Self {
        Self { init, test }
    }
}

impl<
        K: Ord + Clone + DeserializeOwned + Serialize,
        V: Ord + Clone + DeserializeOwned + Serialize,
    > TestCaseTx<K, V>
{
    #[allow(dead_code)]
    pub fn new(
        init: HashMap<OID, Vec<TestOpEnum<K, V>>>,
        test: HashMap<XID, Vec<TestTxOpEnum<K, V>>>,
    ) -> Self {
        Self { init, test }
    }
}
