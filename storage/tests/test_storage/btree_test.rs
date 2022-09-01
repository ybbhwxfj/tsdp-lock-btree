use std::collections::BTreeMap;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Bound;

use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;

use common::id::{gen_xid, OID, XID};

use crate::test_storage::gen_slice::GenStub;
use crate::test_storage::test_operation::{
    TestOpDelete, TestOpEnum, TestOpInsert, TestOpSearchKey, TestOpSearchRange, TestOpUpdate,
    TestTxOpEnum,
};

pub enum TestOpType {
    Insert,
    Update,
    Delete,
    SearchKey,
    SearchRange,
}

#[derive(Clone)]
pub struct RangeRatio {
    pub include: u32,
    pub exclude: u32,
    pub unbound: u32,
}

#[derive(Clone)]
pub struct OpRatio {
    pub update: u32,
    pub insert: u32,
    pub delete: u32,
    pub search_key: u32,
    pub search_range: u32,
}

#[derive(Clone)]
pub struct Ratio {
    pub op_ratio: OpRatio,
    pub range_ratio: (RangeRatio, RangeRatio),
    pub commit: f64,
}

#[derive(Clone)]
pub struct TestSetting<
    K: Ord + Clone + DeserializeOwned + Serialize,
    GK: GenStub<K>,
    V: Ord + Clone + DeserializeOwned + Serialize,
    GV: GenStub<V>,
> {
    phantom: PhantomData<(K, V)>,
    pub gen_key: GK,
    pub gen_value: GV,
    pub max_rows: u64,
}

pub struct BTreeTestGen<
    K: Ord + Clone + DeserializeOwned + Serialize,
    GK: GenStub<K>,
    V: Ord + Clone + DeserializeOwned + Serialize,
    GV: GenStub<V>,
> {
    table_id: OID,
    rnd: ThreadRng,
    tree: BTreeMap<K, V>,
    gen_key: GK,
    gen_value: GV,
    max_rows: u64,
}

pub struct BTreeTestGenSet<
    K: Ord + Clone + DeserializeOwned + Serialize,
    GK: GenStub<K>,
    V: Ord + Clone + DeserializeOwned + Serialize,
    GV: GenStub<V>,
> {
    table_ids: Vec<OID>,
    hash_map_gen: HashMap<OID, BTreeTestGen<K, GK, V, GV>>,
}

impl OpRatio {
    pub fn new(update: u32, insert: u32, delete: u32, search_key: u32, search_range: u32) -> Self {
        Self {
            update,
            insert,
            delete,
            search_key,
            search_range,
        }
    }
}

impl RangeRatio {
    pub fn new(include: u32, exclude: u32, unbound: u32) -> Self {
        Self {
            include,
            exclude,
            unbound,
        }
    }
}

impl Ratio {
    pub fn new(op_ratio: OpRatio, range_ratio: (RangeRatio, RangeRatio)) -> Self {
        let r = Self {
            op_ratio,
            range_ratio,
            commit: 1.0,
        };

        if r.get_total() == 0 {
            panic!("error")
        }
        return r;
    }

    fn get_total(&self) -> u32 {
        self.op_ratio.update
            + self.op_ratio.insert
            + self.op_ratio.delete
            + self.op_ratio.search_key
            + self.op_ratio.search_range
    }

    fn get_range_total_left_right(&self) -> (u32, u32) {
        (
            self.get_range_total::<true>(),
            self.get_range_total::<false>(),
        )
    }

    fn get_range_total<const LEFT: bool>(&self) -> u32 {
        let range = if LEFT {
            &self.range_ratio.0
        } else {
            &self.range_ratio.1
        };
        range.exclude + range.include + range.unbound
    }
}

impl<
        K: Ord + Clone + DeserializeOwned + Serialize,
        GK: GenStub<K>,
        V: Ord + Clone + DeserializeOwned + Serialize,
        GV: GenStub<V>,
    > TestSetting<K, GK, V, GV>
{
    pub fn new(gen_key: GK, gen_value: GV, max_rows: u64) -> Self {
        Self {
            phantom: Default::default(),
            gen_key,
            gen_value,

            max_rows,
        }
    }
}

impl<
        K: Ord + Clone + DeserializeOwned + Serialize,
        GK: GenStub<K>,
        V: Ord + Clone + DeserializeOwned + Serialize,
        GV: GenStub<V>,
    > BTreeTestGen<K, GK, V, GV>
{
    pub fn new(oid: OID, setting: TestSetting<K, GK, V, GV>) -> Self {
        BTreeTestGen {
            table_id: oid,
            rnd: ThreadRng::default(),
            tree: BTreeMap::new(),
            gen_key: setting.gen_key.clone(),
            gen_value: setting.gen_value.clone(),
            max_rows: setting.max_rows,
        }
    }

    pub fn gen_init_data(&mut self) -> Vec<TestOpEnum<K, V>> {
        self.create_init(self.max_rows as usize)
    }

    pub fn gen_test_ops(&mut self, n: usize, ratio: &Ratio) -> Vec<TestOpEnum<K, V>> {
        self.create_test_operations(n, true, &ratio)
    }

    pub fn gen_test_tx_access_op(&mut self, xid: XID, ratio: &Ratio) -> TestTxOpEnum<K, V> {
        self.create_test_tx_access(xid, ratio)
    }

    fn create_init(&mut self, n: usize) -> Vec<TestOpEnum<K, V>> {
        let mut vec = Vec::new();
        for _i in 0..n {
            let k = self.gen_key.gen();
            let v = self.gen_value.gen();
            if !self.tree.contains_key(&k) {
                let _ = self.tree.insert(k.clone(), v.clone());
            }
            let e = TestOpEnum::Insert(TestOpInsert {
                key: k,
                value: v,
                option: Default::default(),
                result: None,
            });
            vec.push(e)
        }
        vec
    }

    fn create_test_operations(
        &mut self,
        n: usize,
        check_result: bool,
        ratio: &Ratio,
    ) -> Vec<TestOpEnum<K, V>> {
        let mut vec = Vec::new();
        for _i in 0..n {
            let ope = self.random_create_operations(check_result, ratio);
            vec.push(ope);
        }
        vec
    }

    fn create_test_tx_access(&mut self, xid: XID, ratio: &Ratio) -> TestTxOpEnum<K, V> {
        let access_op = self.random_create_operations(false, ratio);
        TestTxOpEnum::TTOAccess(xid, self.table_id, access_op)
    }

    fn random_gen_bound_type<const LEFT: bool>(
        rnd: &mut ThreadRng,
        range_ratio: &(RangeRatio, RangeRatio),
        total_num: (u32, u32),
        key: K,
    ) -> Bound<K> {
        let (total, ratio) = if LEFT {
            (total_num.0, &range_ratio.0)
        } else {
            (total_num.1, &range_ratio.1)
        };
        let n = rnd.gen_range(0..total);
        if n < ratio.include {
            Bound::Included(key)
        } else if n < ratio.include + ratio.exclude {
            Bound::Excluded(key)
        } else {
            Bound::Unbounded
        }
    }

    fn random_gen_op_type(rnd: &mut ThreadRng, r: &Ratio) -> TestOpType {
        let ratio = &r.op_ratio;
        let total = r.get_total();
        let n = rnd.gen_range(0..total);
        return if n < ratio.insert {
            TestOpType::Insert
        } else if n < ratio.insert + ratio.update {
            TestOpType::Update
        } else if n < ratio.insert + ratio.update + ratio.delete {
            TestOpType::Delete
        } else if n < ratio.insert + ratio.update + ratio.delete + ratio.search_key {
            TestOpType::SearchKey
        } else {
            TestOpType::SearchRange
        };
    }

    fn random_create_operations(&mut self, check_result: bool, ratio: &Ratio) -> TestOpEnum<K, V> {
        let op_type = Self::random_gen_op_type(&mut self.rnd, ratio);
        let op = match op_type {
            TestOpType::Insert => self.create_insert(check_result),
            TestOpType::Update => self.create_update(check_result),
            TestOpType::SearchKey => self.create_search_key(check_result),
            TestOpType::SearchRange => self.create_search_range(check_result, ratio),
            TestOpType::Delete => self.create_delete(check_result),
        };
        op
    }

    fn create_insert(&mut self, check_result: bool) -> TestOpEnum<K, V> {
        let key = self.gen_key.gen();
        let value = self.gen_value.gen();
        let opt_r = {
            if check_result && !self.tree.contains_key(&key) {
                self.tree.insert(key.clone(), value.clone());
                Some(true)
            } else {
                None
            }
        };

        TestOpEnum::Insert(TestOpInsert {
            key,
            value,
            option: Default::default(),
            result: opt_r,
        })
    }

    fn create_update(&mut self, check_result: bool) -> TestOpEnum<K, V> {
        let key = self.gen_key.gen();
        let value = self.gen_value.gen();
        let opt_r = {
            if check_result && self.tree.contains_key(&key) {
                self.tree.insert(key.clone(), value.clone());
                Some(true)
            } else {
                None
            }
        };
        TestOpEnum::Update(TestOpUpdate {
            key,
            value,
            option: Default::default(),
            result: opt_r,
        })
    }

    fn create_delete(&mut self, check_result: bool) -> TestOpEnum<K, V> {
        let key = self.gen_key.gen();
        let opt_r = {
            if check_result && self.tree.contains_key(&key) {
                self.tree.remove(&key);
                Some(true)
            } else {
                None
            }
        };
        TestOpEnum::Delete(TestOpDelete {
            key,
            option: Default::default(),
            result: opt_r,
        })
    }

    fn create_search_key(&mut self, check_result: bool) -> TestOpEnum<K, V> {
        let key = self.gen_key.gen();
        let opt_r = {
            if check_result && self.tree.contains_key(&key) {
                let mut vec = Vec::new();
                let opt_v = self.tree.get(&key);
                let v = opt_v.unwrap().clone();
                vec.push((key.clone(), v.clone()));
                Some(vec)
            } else {
                None
            }
        };
        TestOpEnum::SearchKey(TestOpSearchKey {
            key,
            option: Default::default(),
            result: opt_r,
        })
    }

    fn create_search_range(&mut self, check_result: bool, ratio: &Ratio) -> TestOpEnum<K, V> {
        let (k1, k2) = loop {
            let k1 = self.gen_key.gen();
            let k2 = self.gen_key.gen();
            let ord = k1.cmp(&k2);
            if ord.is_lt() {
                break (k1, k2);
            } else if ord.is_gt() {
                break (k2, k1);
            }
        };
        assert!(k1 < k2);
        let left_bound = Self::random_gen_bound_type::<true>(
            &mut self.rnd,
            &ratio.range_ratio,
            ratio.get_range_total_left_right(),
            k1,
        );
        let right_bound = Self::random_gen_bound_type::<true>(
            &mut self.rnd,
            &ratio.range_ratio,
            ratio.get_range_total_left_right(),
            k2,
        );

        let opt_r = {
            if check_result {
                let mut vec = Vec::new();
                for (k, v) in self.tree.range((left_bound.clone(), right_bound.clone())) {
                    vec.push((k.clone(), v.clone()));
                }
                Some(vec)
            } else {
                None
            }
        };

        TestOpEnum::SearchRange(TestOpSearchRange {
            low: left_bound,
            up: right_bound,
            option: Default::default(),
            result: opt_r,
        })
    }
}

impl<
        K: Ord + Clone + DeserializeOwned + Serialize,
        GK: GenStub<K>,
        V: Ord + Clone + DeserializeOwned + Serialize,
        GV: GenStub<V>,
    > BTreeTestGenSet<K, GK, V, GV>
{
    pub fn new(test_config: HashMap<OID, TestSetting<K, GK, V, GV>>) -> Self {
        let mut hash_map = HashMap::new();
        let mut table_ids = Vec::new();
        for (oid, config) in &test_config {
            table_ids.push(*oid);
            let _ = hash_map.insert(*oid, BTreeTestGen::new(*oid, config.clone()));
        }
        Self {
            table_ids,
            hash_map_gen: hash_map,
        }
    }

    pub fn gen_init(&mut self) -> HashMap<OID, Vec<TestOpEnum<K, V>>> {
        let mut hash = HashMap::new();
        for (id, gen) in &mut self.hash_map_gen {
            let ops = gen.gen_init_data();
            hash.insert(*id, ops);
        }
        hash
    }

    pub fn gen_test_tx(
        &mut self,
        tx_num: usize,
        min_ops_per_tx: usize,
        max_ops_per_tx: usize,
        ratio: &Ratio,
    ) -> HashMap<XID, Vec<TestTxOpEnum<K, V>>> {
        let mut hash = HashMap::new();
        let mut rnd = thread_rng();

        for _i in 0..tx_num {
            let xid = gen_xid();
            let mut tx_ops = Vec::new();
            tx_ops.push(TestTxOpEnum::TTOBegin(xid));
            let n = rnd.gen_range(min_ops_per_tx as u64..max_ops_per_tx as u64);
            for _j in 0..n {
                let table_id = self.table_ids.choose(&mut rnd).unwrap().clone();
                let opt_gen = self.hash_map_gen.get_mut(&table_id);
                let gen = opt_gen.unwrap();
                let access_op = gen.gen_test_tx_access_op(xid, ratio);
                tx_ops.push(access_op)
            }
            let is_commit = rnd.gen_bool(ratio.commit);
            if is_commit {
                tx_ops.push(TestTxOpEnum::TTOCommit(xid));
            } else {
                tx_ops.push(TestTxOpEnum::TTOAbort(xid));
            }

            hash.insert(xid, tx_ops);
        }
        hash
    }
}
