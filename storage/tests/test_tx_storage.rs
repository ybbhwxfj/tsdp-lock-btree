#![feature(async_closure)]

extern crate core;

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use scc::ebr::Barrier;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tempfile::tempdir;
use tide::Request;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tracing::{debug, debug_span, info, Instrument};
use tracing_subscriber;

use adt::compare::Compare;
use adt::range::RangeBounds;
use common::error_type::ET;
use common::id::{gen_oid, OID, XID};
use common::result::{io_result, Result};
use storage::access::handle_read::EmptyHandleReadKV;
use storage::access::handle_write::EmptyHandleWrite;
use storage::access::schema::TableSchema;
use storage::access::to_json::ToJson;
use storage::storage_handler::StorageHandler;
use storage::table_context::TableContext;
use storage::trans::deadlock::DeadLockDetector;
use storage::trans::history::{History, HistoryEmpty};
use storage::trans::tx_context::{DebugTxContext, TxContext};
use storage::trans::victim_fn::VictimFn;
use storage::trans::write_operation::WriteOp;

use crate::test_storage::btree_test::{BTreeTestGenSet, OpRatio, RangeRatio, Ratio, TestSetting};
use crate::test_storage::debug_server::DebugServer;
use crate::test_storage::gen_slice::{
    IntegerCompare, IntegerGen, IntegerSlice, SliceStub, SliceToJson,
};
use crate::test_storage::opt_test_case::TestCaseOption;
use crate::test_storage::test_case_file::{json_read, json_write, test_case_file_path};
use crate::test_storage::test_case_struct::TestCaseTx;
use crate::test_storage::test_operation::{TestOpEnum, TestTxOpEnum};

pub mod test_storage;

type TestCase = TestCaseOption<TestParam>;

struct TestParam {
    pub num_tx: usize,
    pub num_min_ops_per_tx: usize,
    pub num_max_ops_per_tx: usize,
    // table_id, max rows, max value(0..max_value)
    pub table_max_rows_max_value: Vec<(OID, usize, usize)>,
}

#[derive(Clone)]
struct TestContext {
    tx: Arc<scc::HashIndex<XID, Arc<TxContext>>>,
    dl: Arc<DeadLockDetector>,
}

#[derive(Serialize)]
struct DebugAllTx {
    tx: HashMap<XID, DebugTxContext>,
}

impl TestContext {
    pub fn new(dl: Arc<DeadLockDetector>) -> Self {
        Self {
            tx: Default::default(),
            dl,
        }
    }

    pub async fn get_or_insert(&self, xid: XID) -> Arc<TxContext> {
        let opt = {
            let barrier = Barrier::new();
            let opt = self.tx.read_with(&xid, |_, c| c.clone(), &barrier);
            opt
        };
        match opt {
            Some(c) => c,
            None => {
                let c = Arc::new(TxContext::new(xid));
                let _ = self.tx.insert_async(xid, c.clone()).await;
                c
            }
        }
    }

    pub async fn debug(&self) -> Result<String> {
        let dc = self.dl.debug_deadlock().await;
        let dt = self.debug_all_tx().await;
        let j = serde_json::to_string(&(dc, dt)).unwrap();

        Ok(j)
    }

    async fn debug_all_tx(&self) -> DebugAllTx {
        let vec = {
            let mut vec = Vec::new();
            let b = Barrier::new();
            for (k, v) in self.tx.iter(&b) {
                vec.push((k.clone(), v.clone()));
            }
            vec
        };
        let mut hash_map = HashMap::new();
        for (k, c) in vec {
            let d = c.to_debug_serde().await;
            hash_map.insert(k, d);
        }

        DebugAllTx { tx: hash_map }
    }
}

#[async_trait]
impl VictimFn for TestContext {
    async fn victim(&self, xid: XID) {
        let tx = self.get_or_insert(xid).await;
        tx.notify_abort(Err(ET::Deadlock)).await;
    }
}

#[async_trait]
impl tide::Endpoint<()> for TestContext {
    async fn call(&self, _req: Request<()>) -> tide::Result {
        let r = self.debug().await;
        match r {
            Ok(s) => Ok(s.into()),
            Err(e) => Ok(e.to_string().into()),
        }
    }
}

async fn load_data<
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    K: Ord + Clone + DeserializeOwned + Serialize + Send + Sync + 'static,
    V: Ord + Clone + DeserializeOwned + Serialize + Send + Sync + 'static,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    HI: History,
>(
    handle: Arc<StorageHandler<C, KT, VT, HI>>,
    init_data: HashMap<OID, Vec<TestOpEnum<K, V>>>,
    param: (C, KT, VT),
) -> Result<()> {
    for (table_id, ops) in init_data {
        let mut opt_table = None;
        for _ in 0..2 {
            let r = handle.get_table_handler(table_id).await;
            match r {
                Ok(t) => {
                    opt_table = Some(t);
                    break;
                }
                Err(e) => {
                    match e {
                        ET::NoSuchElement => {
                            let schema = TableSchema {
                                table_id,
                                index_id: gen_oid(),
                                heap_id: gen_oid(),
                                name: table_id.to_string(),
                                columns: vec![],
                            };
                            let ctx = TableContext {
                                key_desc: Default::default(),   // not used
                                value_desc: Default::default(), // not used
                                name2desc: Default::default(),
                                id2desc: Default::default(),
                                compare: param.0.clone(),
                                key2json: param.1.clone(),
                                value2json: param.2.clone(),
                            };

                            handle.create_table(schema, ctx).await?;
                        }
                        _ => {
                            return Err(e);
                        }
                    }
                }
            }
        }
        let table = opt_table.unwrap();
        for op in ops {
            let write_op = match op {
                TestOpEnum::Insert(op) => {
                    let k = Vec::from(KS::from_type(&op.key).as_slice());
                    let v = Vec::from(VS::from_type(&op.value).as_slice());
                    WriteOp::new_insert(table_id, k, v)
                }
                _ => {
                    panic!("not allow");
                }
            };
            table.write_operation(&write_op).await?;
        }
    }
    info!("load table done...");
    Ok(())
}

fn run_test<
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    K: Ord + Clone + DeserializeOwned + Serialize + Send + Sync + 'static,
    V: Ord + Clone + DeserializeOwned + Serialize + Send + Sync + 'static,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    HI: History,
>(
    ctx: TestContext,
    handle: Arc<StorageHandler<C, KT, VT, HI>>,
    case_data: TestCaseTx<K, V>,
    param: (C, KT, VT),
) -> Result<()> {
    let runtime = Builder::new_current_thread()
        .thread_name("main")
        .build()
        .unwrap();

    let local_set1 = LocalSet::new();
    let handle_load = handle.clone();
    local_set1.spawn_local(async move {
        let r = load_data::<C, KT, VT, K, V, KS, VS, HI>(handle_load, case_data.init, param).await;
        assert!(r.is_ok());
    });
    runtime.block_on(local_set1);

    let local_set2 = LocalSet::new();
    for (xid, td) in case_data.test {
        let x = xid;
        let v = td;
        let h = handle.clone();
        let c = ctx.clone();
        let f = || async move {
            let _ = run_tx::<C, KT, VT, K, V, KS, VS, HI>(c, h, x, v).await;
        };

        let _ = local_set2.spawn_local(f());
    }

    runtime.block_on(local_set2);
    info!("test case run done");
    Ok(())
}

fn run_test_storage(
    buff_size: u64,
    page_size: u64,
    base_path: &String,
    case: TestCase,
) -> Result<()> {
    let case = test_case_file(base_path, &case)?;

    let cmp = IntegerCompare::default();
    let k2j = SliceToJson::<u64, IntegerSlice>::default();
    let v2j = SliceToJson::<u64, IntegerSlice>::default();
    let handle = Arc::new(StorageHandler::new(
        buff_size,
        page_size,
        base_path.clone(),
        HistoryEmpty::new(),
    ));
    let ctx = TestContext::new(handle.deadlock_detector().clone());
    let mut server = DebugServer::new("127.0.0.1".to_string(), 8080);
    let debug_handler = ctx.clone();
    let _ = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .thread_name("debug")
            .build()
            .unwrap();
        let t = LocalSet::new();
        t.spawn_local(async move {
            let r = server.register("/".to_string(), debug_handler);
            if r.is_ok() {
                let _ = server.run().await;
            } else {
                panic!("error");
            }
        });
        runtime.block_on(t);
    });

    let victim_fn = ctx.clone();
    let redo_log = handle.redo_log().clone();
    let dl_detector = handle.deadlock_detector().clone();

    let to_stop_dl_detector = dl_detector.clone();
    let to_stop_dl_redo_log = redo_log.clone();
    let thd1 = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .thread_name("detect_and_flush")
            .build()
            .unwrap();
        let t = LocalSet::new();
        t.spawn_local(async move {
            let r = redo_log.flushing_log().await;
            assert!(r.is_ok());
        });
        t.spawn_local(async move {
            let r = dl_detector.detect(victim_fn).await;
            assert!(r.is_ok());
        });
        runtime.block_on(t);
    });
    run_test::<IntegerCompare, _, _, _, _, IntegerSlice, IntegerSlice, _>(
        ctx,
        handle.clone(),
        case,
        (cmp, k2j, v2j),
    )?;

    let thd2 = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .thread_name("stop")
            .build()
            .unwrap();
        let t = LocalSet::new();
        t.spawn_local(async move {
            handle.debug_assert().await;
            to_stop_dl_detector.stop().await;
            to_stop_dl_redo_log.stop().await;
        });
        runtime.block_on(t);
    });

    let _ = thd2.join();
    let _ = thd1.join();
    Ok(())
}

fn gen_test_init_data(param: &TestParam) -> TestCaseTx<u64, u64> {
    let mut settings = HashMap::new();
    for (id, max_rows, max_value) in &param.table_max_rows_max_value {
        let gen_k = IntegerGen::new((0, *max_value as u64));
        let gen_v = IntegerGen::new((0, *max_value as u64));
        let setting = TestSetting::new(gen_k, gen_v, *max_rows as u64);
        if settings.contains_key(id) {
            panic!("existing such table id");
        }
        settings.insert(*id, setting);
    }

    let mut bt_test_gen = BTreeTestGenSet::<u64, IntegerGen, u64, IntegerGen>::new(settings);
    let init_ops = bt_test_gen.gen_init();

    let rr_l = RangeRatio::new(2, 2, 1);
    let rr_r = RangeRatio::new(2, 2, 1);
    let or = OpRatio::new(1, 1, 1, 2, 1);
    let ratio = Ratio::new(or, (rr_l, rr_r));

    let test_ops = bt_test_gen.gen_test_tx(
        param.num_tx,
        param.num_min_ops_per_tx,
        param.num_max_ops_per_tx,
        &ratio,
    );
    return TestCaseTx::new(init_ops, test_ops);
}

fn test_case_file(base_path: &String, test_case: &TestCase) -> Result<TestCaseTx<u64, u64>> {
    let test = match test_case {
        TestCaseOption::FromFile(name) => {
            let path = test_case_file_path(name.clone())?;
            info!("read test case file {}", path);
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    return Err(ET::IOError(e.to_string()));
                }
            };
            let test: TestCaseTx<u64, u64> = json_read(file)?;
            test
        }
        TestCaseOption::GenParam(p) => {
            let test = gen_test_init_data(&p);
            let p = PathBuf::from(base_path.clone());
            let path = p.join("test_tx_storage.json");
            info!("generate test case file {}", path.to_str().unwrap());
            let r = File::create(path);
            let file = io_result(r)?;
            json_write(file, &test)?;
            test
        }
    };
    Ok(test)
}

async fn run_tx<C, KT, VT, K, V, KS, VS, HI>(
    ctx: TestContext,
    handler: Arc<StorageHandler<C, KT, VT, HI>>,
    xid: XID,
    ops: Vec<TestTxOpEnum<K, V>>,
) -> Result<()>
where
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    K: Ord + Clone + DeserializeOwned + Serialize + 'static,
    V: Ord + Clone + DeserializeOwned + Serialize + 'static,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    HI: History,
{
    let ctx = ctx.get_or_insert(xid).await;
    debug!("run tx {}", ctx.xid());
    for op in ops {
        let r = run_tx_op::<C, KT, VT, K, V, KS, VS, HI>(&handler, &ctx, op).await;
        match r {
            Err(e) => {
                match e {
                    ET::Deadlock => {
                        debug!("deadlock victim {}", ctx.xid());
                    }
                    _ => {
                        panic!("error, {:?}", e)
                    }
                }
                handler.abort_tx(&ctx).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn run_tx_op<C, KT, VT, K, V, KS, VS, HI>(
    handler: &StorageHandler<C, KT, VT, HI>,
    tx: &TxContext,
    op: TestTxOpEnum<K, V>,
) -> Result<()>
where
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    K: Ord + Clone + DeserializeOwned + Serialize + 'static,
    V: Ord + Clone + DeserializeOwned + Serialize + 'static,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    HI: History,
{
    if !tx.get_state().await.is_running() {
        return Ok(());
    }
    debug!("tx operation {}", serde_json::to_string(&op).unwrap());
    match op {
        TestTxOpEnum::TTOBegin(_) => {
            handler.begin_tx(tx).await?;
        }
        TestTxOpEnum::TTOCommit(_) => {
            handler.abort_tx(tx).await?;
        }
        TestTxOpEnum::TTOAbort(_) => {
            handler.commit_tx(tx).await?;
        }
        TestTxOpEnum::TTOAccess(_, table_id, op) => {
            let r = run_tx_op_access::<C, KT, VT, K, V, KS, VS, HI>(handler, tx, table_id, op)
                .instrument(debug_span!("run_tx_op_access"))
                .await;
            if r.is_err() {
                debug!("run tx op access error");
            }
            r?;
        }
    }
    Ok(())
}

async fn run_tx_op_access<C, KT, VT, K, V, KS, VS, HI>(
    handler: &StorageHandler<C, KT, VT, HI>,
    tx: &TxContext,
    table_id: OID,
    op: TestOpEnum<K, V>,
) -> Result<()>
where
    C: Compare<[u8]> + 'static,
    KT: ToJson + 'static,
    VT: ToJson + 'static,
    K: Ord + Clone + DeserializeOwned + Serialize,
    V: Ord + Clone + DeserializeOwned + Serialize,
    KS: SliceStub<K>,
    VS: SliceStub<V>,
    HI: History,
{
    debug!("tx access operation {}", tx.xid());
    match op {
        TestOpEnum::Insert(o) => {
            let key = KS::from_type(&o.key);
            let value = VS::from_type(&o.value);
            let _ = handler
                .insert(
                    tx,
                    table_id,
                    &key,
                    &value,
                    o.option.clone(),
                    &EmptyHandleWrite::new(),
                )
                .await?;
        }
        TestOpEnum::Update(o) => {
            let key = KS::from_type(&o.key);
            let value = VS::from_type(&o.value);
            let _ = handler
                .update(
                    tx,
                    table_id,
                    &key,
                    &value,
                    o.option.clone(),
                    &EmptyHandleWrite::new(),
                )
                .await?;
        }
        TestOpEnum::Delete(o) => {
            let key = KS::from_type(&o.key);
            let _ = handler
                .delete(
                    tx,
                    table_id,
                    &key,
                    o.option.clone(),
                    &EmptyHandleWrite::new(),
                )
                .await?;
        }
        TestOpEnum::SearchKey(o) => {
            let key = KS::from_type(&o.key);
            let _ = handler
                .search_key(
                    tx,
                    table_id,
                    &key,
                    o.option.clone(),
                    &EmptyHandleReadKV::new(),
                )
                .await?;
        }
        TestOpEnum::SearchRange(o) => {
            let range = RangeBounds::new(o.low.clone(), o.up.clone());
            let f_map = |k: &K| -> KS { KS::from_type(k) };
            let search_range = range.map(&f_map);
            let _ = handler
                .search_range(
                    tx,
                    table_id,
                    &search_range,
                    o.option.clone(),
                    &EmptyHandleReadKV::new(),
                )
                .await?;
        }
    }
    debug!("tx {} access operation done", tx.xid());
    Ok(())
}

#[test]
fn test_storage() {
    //console_subscriber::init();

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
    let max_value = 1000;
    let max_rows = max_value / 2;
    for test_case in [
        // read test case from json file
        // TestCase::FromFile("test_case_tx.json".to_string()),
        // generate test case with parameter
        TestCase::GenParam(TestParam {
            num_tx: 20000,
            num_min_ops_per_tx: 10,
            num_max_ops_per_tx: 20,
            table_max_rows_max_value: vec![(1, max_rows, max_value), (2, max_rows, max_value)],
        }),
    ] {
        let r = run_test_storage(buff_size, page_size, &base_path, test_case);
        assert!(r.is_ok());
    }
}
