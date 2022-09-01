use std::fmt::Display;
use std::fs::File;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use json::{object, JsonValue};
use log::{error, info, trace};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tempfile::tempdir;
use tokio::runtime::Builder;
use tracing::{trace_span, Instrument};

use adt::compare::Compare;
use adt::range::RangeBounds;
use common::error_type::ET;
use common::result::{io_result, Result};

use storage::access::access_opt::{DeleteOption, InsertOption, SearchOption, UpdateOption};
use storage::access::btree_file::BTreeFile;
use storage::access::btree_index::BTreeIndex;
use storage::access::buf_pool::BufPool;
use storage::access::handle_read::HandleReadKV;
use storage::access::handle_write::EmptyHandleWrite;

use storage::access::to_json::ToJson;

use crate::test_storage::btree_test::{BTreeTestGen, OpRatio, RangeRatio, Ratio, TestSetting};
use crate::test_storage::gen_slice::{
    IntegerCompare, IntegerGen, IntegerSlice, SliceStub, SliceToJson,
};
use crate::test_storage::opt_test_case::TestCaseOption;
use crate::test_storage::test_case_file::{json_read, json_write, test_case_file_path};
use crate::test_storage::test_case_struct::TestCaseNonTx;
use crate::test_storage::test_operation::TestOpEnum;

pub mod test_storage;

struct TestParam {
    max_value: u64,
    num_table_rows: u64,
    num_operations: u64,
    operation_ratio: Ratio,
}

enum BTreeClusterOrNot<C: Compare<[u8]> + 'static, KT: ToJson, VT: ToJson> {
    BTreeCluster(BTreeFile<C, KT, VT>),
    BTreeNonCluster(BTreeIndex<C, KT, VT>),
}

struct HSearch {
    vec: Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>,
}

impl Clone for HSearch {
    fn clone(&self) -> Self {
        Self {
            vec: self.vec.clone(),
        }
    }
}

#[async_trait]
impl HandleReadKV for HSearch {
    async fn on_read(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() == 0 {
            panic!("error")
        }
        let mut guard = self.vec.lock().unwrap();
        guard.push((Vec::from(key), Vec::from(value)));
        Ok(())
    }
}

fn change_slice_to_stub<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    V: Clone + DeserializeOwned + Serialize,
    VS: SliceStub<V>,
>(
    array: &Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(K, V)> {
    let mut vec = Vec::new();
    for (k, v) in array {
        let ks = KS::from_slice(k.as_slice()).to_type();
        let vs = VS::from_slice(v.as_slice()).to_type();
        vec.push((ks, vs));
    }
    vec
}

async fn test_btree_operation<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    C: Compare<[u8]>,
    T: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, T, VT>,
    ops: &Vec<TestOpEnum<K, V>>,
) {
    for o in ops {
        match btree {
            BTreeClusterOrNot::BTreeCluster(t) => {
                let json = t.to_json().await.unwrap();
                trace!("result: {}", json.dump());
            }
            BTreeClusterOrNot::BTreeNonCluster(t) => {
                let json = t.to_json().await.unwrap();
                trace!("result: {}", json.dump());
            }
        }

        match o {
            TestOpEnum::Insert(op) => {
                bt_insert::<K, V, KS, VS, C, T, VT>(
                    btree, &op.key, &op.value, &op.option, &op.result,
                )
                .await;
            }
            TestOpEnum::Update(op) => {
                bt_update::<K, V, KS, VS, C, T, VT>(
                    btree, &op.key, &op.value, &op.option, &op.result,
                )
                .await;
            }
            TestOpEnum::Delete(op) => {
                bt_delete::<K, KS, C, T, VT>(btree, &op.key, &op.option, &op.result).await;
            }
            TestOpEnum::SearchKey(op) => {
                bt_search_key::<K, V, KS, VS, C, T, VT>(btree, &op.key, &op.option, &op.result)
                    .await;
            }
            TestOpEnum::SearchRange(op) => {
                let range = RangeBounds::new(op.low.clone(), op.up.clone());
                bt_search_range::<K, V, KS, VS, C, T, VT>(btree, &range, &op.option, &op.result)
                    .await;
            }
        }
    }
}

async fn bt_insert<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    C: Compare<[u8]>,
    KT: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, KT, VT>,
    key: &K,
    value: &V,
    opt: &InsertOption,
    res: &Option<bool>,
) {
    trace!("insert key {}, value {}", key, value);
    let ks = KS::from_type(key);
    let vs = VS::from_type(value);
    let r = match btree {
        BTreeClusterOrNot::BTreeCluster(t) => {
            t.insert(
                &ks,
                &vs,
                opt.clone(),
                &EmptyHandleWrite::default(),
                &mut None,
            )
            .await
        }
        BTreeClusterOrNot::BTreeNonCluster(t) => {
            t.insert(
                &ks,
                &vs,
                opt.clone(),
                &EmptyHandleWrite::default(),
                &mut None,
            )
            .await
        }
    };

    match res {
        Some(t) => {
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), *t);
        }
        None => {}
    }
}

async fn bt_update<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    C: Compare<[u8]>,
    T: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, T, VT>,
    key: &K,
    value: &V,
    opt: &UpdateOption,
    res: &Option<bool>,
) {
    trace!("update key {}, value {}", key, value);
    let ks = KS::from_type(key);
    let vs = VS::from_type(value);
    let r = match btree {
        BTreeClusterOrNot::BTreeCluster(t) => {
            t.update(
                &ks,
                &vs,
                opt.clone(),
                &EmptyHandleWrite::default(),
                &mut None,
            )
            .await
        }
        BTreeClusterOrNot::BTreeNonCluster(t) => {
            t.update(
                &ks,
                &vs,
                opt.clone(),
                &EmptyHandleWrite::default(),
                &mut None,
            )
            .await
        }
    };

    match res {
        Some(t) => {
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), *t);
        }
        None => {}
    }
}

async fn bt_delete<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    C: Compare<[u8]>,
    T: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, T, VT>,
    key: &K,
    opt: &DeleteOption,
    res: &Option<bool>,
) {
    trace!("delete key {}", key);
    let ks = KS::from_type(key);
    let r = match btree {
        BTreeClusterOrNot::BTreeCluster(t) => {
            t.delete(&ks, opt.clone(), &EmptyHandleWrite::default(), &mut None)
                .await
        }
        BTreeClusterOrNot::BTreeNonCluster(t) => {
            t.delete(&ks, opt.clone(), &EmptyHandleWrite::default(), &mut None)
                .await
        }
    };

    match res {
        Some(t) => {
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), *t);
        }
        None => {}
    }
}

fn check_search_result<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
>(
    result: &Vec<(K, V)>,
    golden: &Vec<(K, V)>,
) {
    if result.len() != golden.len() {
        let mut s1 = String::new();
        for (k, v) in result {
            s1 = format!("{} [{}, {}]", s1, k, v);
        }
        let mut s2 = String::new();
        for (k, v) in golden {
            s2 = format!("{} [{}, {}]", s2, k, v);
        }
        trace!("result: {}", s1);
        trace!("golden: {}", s2);
        assert_eq!(result.len(), golden.len());
        return;
    }
    for i in 0..result.len() {
        let (k1, v1) = result.get(i).unwrap();
        let (k2, v2) = golden.get(i).unwrap();

        assert!(k1.eq(k2) && v1.eq(v2));
    }
}

async fn bt_search_key<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    C: Compare<[u8]>,
    T: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, T, VT>,
    key: &K,
    opt: &SearchOption,
    res: &Option<Vec<(K, V)>>,
) {
    trace!("search key {}", key);
    let handle_read = HSearch {
        vec: Default::default(),
    };
    let ks = KS::from_type(key);
    let r = match btree {
        BTreeClusterOrNot::BTreeCluster(t) => {
            t.search_key(&ks, opt.clone(), &handle_read, &mut None)
                .await
        }
        BTreeClusterOrNot::BTreeNonCluster(t) => {
            t.search_key(&ks, opt.clone(), &handle_read, &mut None)
                .await
        }
    };

    match res {
        Some(golden) => {
            assert!(r.is_ok(), "btree test search key");
            let n = r.unwrap();
            let guard = handle_read.vec.lock().unwrap();
            assert_eq!(n, guard.len() as u32);
            let res_vec = change_slice_to_stub::<K, KS, V, VS>(&(*guard));
            check_search_result(&res_vec, golden)
        }
        None => {}
    }
}

async fn bt_search_range<
    K: Ord + Clone + DeserializeOwned + Serialize + Display,
    V: Ord + Clone + DeserializeOwned + Serialize + Display,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    C: Compare<[u8]>,
    T: ToJson,
    VT: ToJson,
>(
    btree: &BTreeClusterOrNot<C, T, VT>,
    range: &RangeBounds<K>,
    opt: &SearchOption,
    res: &Option<Vec<(K, V)>>,
) {
    let to_json = |b: &Bound<K>| -> JsonValue {
        match b {
            Bound::Included(k) => {
                object! {
                    "bound":"included",
                    "value":KS::from_type(k).to_json()
                }
            }
            Bound::Excluded(k) => {
                object! {
                    "bound":"excluded",
                    "value":KS::from_type(k).to_json()
                }
            }
            Bound::Unbounded => JsonValue::Null,
        }
    };

    trace!(
        "search range [{}, {}]",
        to_json(range.low()).dump(),
        to_json(range.up()).dump()
    );

    let search = HSearch {
        vec: Default::default(),
    };

    let f_map = |k: &K| -> KS { KS::from_type(k) };
    let search_range = range.map(&f_map);

    let r = match btree {
        BTreeClusterOrNot::BTreeCluster(t) => {
            t.search_range(&search_range, opt.clone(), &search, &mut None)
                .await
        }
        BTreeClusterOrNot::BTreeNonCluster(t) => {
            t.search_range(&search_range, opt.clone(), &search, &mut None)
                .await
        }
    };

    let _n = match r {
        Ok(n) => n,
        Err(e) => {
            error!("search range error {}", e.to_string());
            return;
        }
    };
    match res {
        Some(golden) => {
            let guard = search.vec.lock().unwrap();
            //assert_eq!(n as usize, golden.len());
            let res_vec = change_slice_to_stub::<K, KS, V, VS>(&(*guard));
            check_search_result(&res_vec, golden)
        }
        None => {}
    }
}

fn test_btree_gen_int_data(
    param: &TestParam,
) -> (Vec<TestOpEnum<u64, u64>>, Vec<TestOpEnum<u64, u64>>) {
    let max_value = param.max_value;
    let operations = param.num_operations;
    let table_rows = param.num_table_rows;
    let gen_k = IntegerGen::new((0, max_value));
    let gen_v = IntegerGen::new((0, max_value));

    let ratio = param.operation_ratio.clone();
    let setting = TestSetting::new(gen_k, gen_v, table_rows);
    let mut bt_test = BTreeTestGen::<u64, IntegerGen, u64, IntegerGen>::new(0, setting);

    let init_ops = bt_test.gen_init_data();
    let test_ops = bt_test.gen_test_ops(operations as usize, &ratio);
    return (init_ops, test_ops);
}

async fn test_btree_file(
    buff_size: u64,
    page_size: u64,
    base_path: String,
    case: TestCaseOption<TestParam>,
    is_cluster_index: bool,
) -> Result<()> {
    let test = match case {
        TestCaseOption::FromFile(name) => {
            let path = test_case_file_path(name)?;
            info!("read test case file {}", path);
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    return Err(ET::IOError(e.to_string()));
                }
            };
            let test: TestCaseNonTx<u64, u64> = json_read(file)?;
            test
        }
        TestCaseOption::GenParam(p) => {
            let (init_ops, test_ops) = test_btree_gen_int_data(&p);
            let test = TestCaseNonTx::new(init_ops, test_ops);
            let p = PathBuf::from(base_path.clone());
            let path = p.join("test_case_non_tx.json");
            info!("generate test case file {}", path.to_str().unwrap());
            let r = File::create(path);

            let file = io_result(r)?;

            json_write(file, &test)?;
            test
        }
    };

    let cmp = IntegerCompare::default();
    let k2j = SliceToJson::<u64, IntegerSlice>::default();
    let v2j = SliceToJson::<u64, IntegerSlice>::default();
    let pool = Arc::new(BufPool::new(buff_size, page_size, base_path));

    let btree = if is_cluster_index {
        let btree = BTreeFile::new(cmp, k2j, v2j, 1, pool.clone()).enable_debug_latch(true);
        info!("test btree cluster index");
        btree.open().await?;

        BTreeClusterOrNot::BTreeCluster(btree)
    } else {
        info!("test btree non cluster index");
        let btree = BTreeIndex::new(cmp, k2j, v2j, 1, 1000, pool.clone()).enable_debug_latch(true);
        btree.open().await?;
        BTreeClusterOrNot::BTreeNonCluster(btree)
    };

    test_btree_operation::<
        u64,
        u64,
        IntegerSlice,
        IntegerSlice,
        IntegerCompare,
        SliceToJson<u64, IntegerSlice>,
        SliceToJson<u64, IntegerSlice>,
    >(&btree, &test.init)
    .await;

    test_btree_operation::<
        u64,
        u64,
        IntegerSlice,
        IntegerSlice,
        IntegerCompare,
        SliceToJson<u64, IntegerSlice>,
        SliceToJson<u64, IntegerSlice>,
    >(&btree, &test.test)
    .await;

    Ok(())
}

fn run_test(
    buff_size: u64,
    page_size: u64,
    base_path: String,
    is_cluster_index: bool,
    case: TestCaseOption<TestParam>,
) -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local.spawn_local(async move {
        let r = test_btree_file(buff_size, page_size, base_path, case, is_cluster_index)
            .instrument(trace_span!("test_btree"))
            .await;
        match r {
            Ok(()) => {}
            Err(e) => {
                info!("error, {}", e.to_string())
            }
        }
    });
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(local);

    Ok(())
}

#[test]
fn test() {
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

    let dir = tempdir().unwrap();
    let base_path = dir.path().to_str().unwrap().to_string();
    let buff_size = 1024 * 1024 * 1024;
    let page_size = 1024;

    let rr_l = RangeRatio::new(2, 2, 1);
    let rr_r = RangeRatio::new(2, 2, 1);
    let or = OpRatio::new(1, 1, 1, 2, 1);
    let ratio = Ratio::new(or, (rr_l, rr_r));

    for is_cluster_index in [
        true,  // test cluster index(BTreeFile)
        false, // test for non cluster index(BTreeIndex)
    ] {
        for case in [
            // generate test data
            TestCaseOption::GenParam({
                TestParam {
                    max_value: 1000,
                    num_table_rows: 800,
                    num_operations: 5000,
                    operation_ratio: ratio.clone(),
                }
            }),
            // read test data from a json file
            TestCaseOption::FromFile("test_case_non_tx.json".to_string()),
        ] {
            let e = run_test(
                buff_size,
                page_size,
                base_path.clone(),
                is_cluster_index,
                case,
            );
            match e {
                Ok(()) => {}
                Err(e) => {
                    error!("run test error :{}", e.to_string())
                }
            }
        }
    }
}
