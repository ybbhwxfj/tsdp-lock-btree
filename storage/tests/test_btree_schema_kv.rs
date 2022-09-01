#![feature(fmt_internals)]

use async_std::sync::Mutex;
use async_trait::async_trait;

use log::info;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use std::collections::HashSet;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use tide::Request;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

use tracing::{error, trace_span, Instrument};

use crate::test_storage::generate::{generate_key, generate_non_exist_key};
use adt::slice::SliceRef;
use common::error_type::ET;
use common::id::{TaskId, XID};
use common::result::io_result;
use common::result::Result;
use storage::access::access_opt::{DeleteOption, InsertOption, SearchOption, UpdateOption};
use storage::access::bt_key_raw::BtKeyRawCmp;
use storage::access::btree_file::BTreeFile;
use storage::access::buf_pool::BufPool;
use storage::access::file_id::FileId;
use storage::access::handle_page::HandlePage;
use storage::access::handle_read::EmptyHandleReadKV;
use storage::access::handle_write::EmptyHandleWrite;
use storage::access::page_id::PageId;
use storage::access::page_latch::PageGuardEP;

use crate::test_storage::debug_server::DebugServer;
use storage::access::tuple_desc::TupleDesc;
use storage::access::tuple_items::TupleItems;
use storage::access::tuple_oper::TupleOper;
use storage::access::tuple_raw::TupleRaw;
use storage::access::tuple_raw_mut::TupleRawMut;
use storage::access::tuple_to_json::TupleToJson;
use storage::d_type::data_type_func::map_name_to_data_type;

use storage::trans::deadlock::TxWaitSet;

use crate::test_storage::opt_test_case::TestCaseOption;
use crate::test_storage::read_csv::{
    create_csv_file, read_from_csv_file, string_record_to_key_value, write_csv_row,
};
use crate::test_storage::schema::load_schema;
use crate::test_storage::test_case_file::test_case_file_path;

pub mod test_storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestOpItems {
    TOInsert(bool, TupleItems, TupleItems),
    TOUpdate(bool, TupleItems, TupleItems),
    TODelete(bool, TupleItems),
    TOSearch(bool, TupleItems),
}

#[derive(Clone, Serialize, Deserialize)]
struct TestCaseItems {
    test_remove_all: bool,
    concurrent: u64,
    init_kv: Vec<TestOpItems>,
    // insert key value pairs
    operations: Vec<TestOpItems>, // insert/delete/update/search
}

#[derive(Clone)]
struct Test {
    btree: Arc<BTreeFile<BtKeyRawCmp, TupleToJson, TupleToJson>>,
    pool: Arc<BufPool>,
    output: Arc<Mutex<String>>,
    tx_wait: Arc<Mutex<TxWaitSet>>,
}

unsafe impl Send for Test {}
unsafe impl Sync for Test {}

fn create_test_csv_file(csv_path: String, num_keys: u64) -> Result<()> {
    let schema_file_name = "schema_int_key.json".to_string();
    let schema = load_schema(schema_file_name)?;
    let map = map_name_to_data_type();
    let (key_desc, value_desc) = TupleDesc::from_table_schema(&schema, &map)?;
    let mut file = read_from_csv_file("data.csv".to_string())?;
    let mut writer = create_csv_file(csv_path)?;

    let mut rnd = ThreadRng::default();
    let mut key_buffer: Vec<u8> = Vec::new();
    let mut value_buffer: Vec<u8> = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut key_set = HashSet::new();
    key_buffer.resize(8192, 0);
    value_buffer.resize(8192, 0);
    for rcd in file.records() {
        let str_rcd = match rcd {
            Ok(r) => r,
            Err(e) => {
                return Err(ET::CSVError(e.to_string()));
            }
        };

        let (_key, value) = string_record_to_key_value(
            &str_rcd,
            &mut key_buffer,
            &mut value_buffer,
            &key_desc,
            &value_desc,
        )?;

        values.push(Vec::from(value.slice()))
    }

    for _ in 0..num_keys {
        let key = generate_key(&key_desc, &mut key_buffer, &mut rnd, &mut key_set)?;
        let value = match values.choose(&mut rnd) {
            Some(v) => TupleRaw::from_slice(v.as_slice()),
            None => {
                return Err(ET::NoneOption);
            }
        };
        write_csv_row(
            key.slice(),
            value.slice(),
            &key_desc,
            &value_desc,
            &mut writer,
        )?;
    }
    Ok(())
}

fn generate_test_case(
    num_keys: u64,
    output_test_case_file: String,
    csv_path: String,
    schema_file_name: String,
    test_remove_all: bool,
    concurrent: u64,
) -> Result<()> {
    let create_r = File::create(output_test_case_file);
    let output_file = io_result(create_r)?;

    let schema = load_schema(schema_file_name)?;
    let map = map_name_to_data_type();
    let (key_desc, value_desc) = TupleDesc::from_table_schema(&schema, &map)?;
    let mut file = read_from_csv_file(csv_path)?;
    let mut key_buffer: Vec<u8> = Vec::new();
    let mut value_buffer: Vec<u8> = Vec::new();

    key_buffer.resize(8192, 0);
    value_buffer.resize(8192, 0);
    let mut ops_run = vec![];
    let mut ops_init = vec![];
    let mut key_set1 = HashSet::new();
    let mut key_set2 = HashSet::new();
    let mut key_set3 = HashSet::new();
    let mut values = Vec::new();
    let mut keys = Vec::new();
    for rcd in file.records() {
        let str_rcd = match rcd {
            Ok(r) => r,
            Err(e) => {
                return Err(ET::CSVError(e.to_string()));
            }
        };

        let (key, value) = string_record_to_key_value(
            &str_rcd,
            &mut key_buffer,
            &mut value_buffer,
            &key_desc,
            &value_desc,
        )?;

        let key_items = TupleItems::from_slice(key.slice(), key_desc.tuple_oper())?;
        let value_items = TupleItems::from_slice(value.slice(), value_desc.tuple_oper())?;
        key_set1.insert(key_items.clone());
        keys.push(key_items.clone());
        values.push(value_items.clone());
        let op = TestOpItems::TOInsert(false, key_items, value_items);
        ops_init.push(op);
    }

    let mut rnd = ThreadRng::default();
    for _ in 0..num_keys {
        let _ = generate_non_exist_key(
            &key_desc,
            &mut key_buffer,
            &mut rnd,
            &mut key_set1,
            &mut key_set2,
        )?;
    }

    let mut key_all = key_set1.clone();
    for k in key_set2.iter() {
        key_all.insert(k.clone());
    }

    for _ in 0..num_keys / 2 {
        let _ = generate_non_exist_key(
            &key_desc,
            &mut key_buffer,
            &mut rnd,
            &mut key_all,
            &mut key_set3,
        )?;
    }
    let keys_non_exist = Vec::from_iter(key_set2.iter().cloned());
    let keys_insert = Vec::from_iter(key_set3.iter().cloned());
    let keys_remove = if !test_remove_all {
        keys.split_off(keys.len() / 2)
    } else {
        keys.clone()
    };

    for (vec, exist) in [(&keys, true), (&keys_insert, false)] {
        for k in vec.iter() {
            let v = match values.choose(&mut rnd) {
                Some(t) => t,
                None => {
                    return Err(ET::NoneOption);
                }
            };
            let op = TestOpItems::TOInsert(exist, k.clone(), v.clone());
            ops_run.push(op);
        }
    }

    for (vec, exist) in [(&keys, true), (&keys_non_exist, false)] {
        for k in vec.iter() {
            // existing ...
            let op = TestOpItems::TOSearch(exist, k.clone());
            ops_run.push(op);
        }
    }

    for (vec, exist) in [(&keys, true), (&keys_non_exist, false)] {
        for k in vec.iter() {
            let v = match values.choose(&mut rnd) {
                Some(t) => t,
                None => {
                    return Err(ET::NoneOption);
                }
            };
            let op = TestOpItems::TOUpdate(exist, k.clone(), v.clone());
            ops_run.push(op);
        }
    }

    for (vec, exist) in [(&keys_remove, true), (&keys_non_exist, false)] {
        for k in vec.iter() {
            let op = TestOpItems::TODelete(exist, k.clone());
            ops_run.push(op);
        }
    }

    if !test_remove_all {
        ops_run.shuffle(&mut rnd);
    }

    let case = TestCaseItems {
        test_remove_all,
        concurrent,
        init_kv: ops_init,
        operations: ops_run,
    };

    match serde_json::to_writer_pretty(output_file, &case) {
        Ok(()) => {}
        Err(e) => {
            return Err(ET::JSONError(e.to_string()));
        }
    }
    Ok(())
}

fn test_case_read(test_case_file: String) -> Result<TestCaseItems> {
    let mut file = match File::open(test_case_file) {
        Ok(f) => f,
        Err(e) => {
            return Err(ET::IOError(e.to_string()));
        }
    };
    let mut json_string = String::new();
    match file.read_to_string(&mut json_string) {
        Ok(_) => {}
        Err(e) => {
            return Err(ET::IOError(e.to_string()));
        }
    }

    let vec = match serde_json::from_str(json_string.as_str()) {
        Ok(v) => v,
        Err(e) => {
            return Err(ET::JSONError(e.to_string()));
        }
    };
    return Ok(vec);
}

async fn test_btree_op(
    btree: &BTreeFile<BtKeyRawCmp, TupleToJson, TupleToJson>,
    op: &TestOpItems,
    value_buffer: &mut Vec<u8>,
    key_buffer: &mut Vec<u8>,
    key_desc: &TupleOper,
    value_desc: &TupleOper,
) -> Result<()> {
    match op {
        TestOpItems::TOInsert(_exist, k, v) => {
            let key = TupleRawMut::from_typed_items(key_buffer, &key_desc, k.items())?;
            let value = TupleRawMut::from_typed_items(value_buffer, &value_desc, v.items())?;

            let _ok = btree
                .insert(
                    &SliceRef::new(key.as_slice()),
                    &SliceRef::new(value.as_slice()),
                    InsertOption::default(),
                    &EmptyHandleWrite::default(),
                    &mut None,
                )
                .await?;
        }
        TestOpItems::TOUpdate(exist, k, v) => {
            let key = TupleRawMut::from_typed_items(key_buffer, &key_desc, k.items())?;
            let value = TupleRawMut::from_typed_items(value_buffer, &value_desc, v.items())?;

            let ok = btree
                .update(
                    &SliceRef::new(key.as_slice()),
                    &SliceRef::new(value.as_slice()),
                    UpdateOption::default(),
                    &EmptyHandleWrite::default(),
                    &mut None,
                )
                .await?;
            assert_eq!(ok, *exist);
        }
        TestOpItems::TODelete(exist, k) => {
            let key = TupleRawMut::from_typed_items(key_buffer, &key_desc, k.items())?;
            let ok = btree
                .delete(
                    &SliceRef::new(key.as_slice()),
                    DeleteOption::default(),
                    &EmptyHandleWrite::default(),
                    &mut None,
                )
                .await?;
            assert_eq!(ok, *exist);
        }
        TestOpItems::TOSearch(exist, k) => {
            let key = TupleRawMut::from_typed_items(key_buffer, &key_desc, k.items())?;

            let async_fn_exist = EmptyHandleReadKV::new();
            let r = btree
                .search_key(
                    &SliceRef::new(key.as_slice()),
                    SearchOption::default(),
                    &async_fn_exist,
                    &mut None,
                )
                .await;
            match r {
                Ok(n) => {
                    if *exist {
                        assert_eq!(n, 1);
                    } else {
                        assert_eq!(n, 0);
                    }
                }
                Err(_e) => {}
            }
        }
    }
    Ok(())
}

async fn test_btree_operations(
    btree: Arc<BTreeFile<BtKeyRawCmp, TupleToJson, TupleToJson>>,
    concurrency: u64,
    ops: Vec<TestOpItems>,
    key_buffer_size: usize,
    value_buffer_size: usize,
    key_desc: TupleOper,
    value_desc: TupleOper,
) -> Result<()> {
    let mut key_buffer: Vec<u8> = Vec::new();
    let mut value_buffer: Vec<u8> = Vec::new();

    key_buffer.resize(key_buffer_size, 0);
    value_buffer.resize(value_buffer_size, 0);

    let check_n = ops.len() / 10;
    for (n, op) in ops.iter().enumerate() {
        if concurrency == 1 && (check_n > 0 && n > 0 && n % check_n == 0) {
            btree.check_invariant().await?;
        }
        test_btree_op(
            &btree,
            op,
            &mut key_buffer,
            &mut value_buffer,
            &key_desc,
            &value_desc,
        )
        .await?;
    }
    let _ = btree.to_json().await?;
    Ok(())
}

async fn test_btree_input_from_json(
    buff_size: u64,
    page_size: u64,
    base_path: String,
    test_case_file: String,
    schema_file_name: String,
) -> Result<()> {
    let schema = load_schema(schema_file_name)?;
    let map = map_name_to_data_type();
    let (key_desc, value_desc) = TupleDesc::from_table_schema(&schema, &map)?;
    let pool = Arc::new(BufPool::new(buff_size, page_size, base_path.clone()));
    let key_cmp = BtKeyRawCmp::new(key_desc.tuple_oper().clone());
    let key2json = TupleToJson::new(key_desc.tuple_oper().clone());
    let value2json = TupleToJson::new(value_desc.tuple_oper().clone());
    let btree = Arc::from(
        BTreeFile::new(
            key_cmp,
            key2json.clone(),
            value2json.clone(),
            1 as FileId,
            pool.clone(),
        )
        .enable_debug_latch(true),
    );
    let test = Test {
        btree: btree.clone(),
        pool,
        output: Default::default(),
        tx_wait: Default::default(),
    };
    let mut server = DebugServer::new("127.0.0.1".to_string(), 8080);
    let _debug_thread = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .thread_name("debug")
            .build()
            .unwrap();
        let t = LocalSet::new();
        t.spawn_local(async move {
            let r = server.register("/".to_string(), test);
            if r.is_ok() {
                let _ = server.run().await;
            } else {
                panic!("error");
            }
        });
        runtime.block_on(t);
    });
    btree.open().await?;

    info!("------ read test case ------");

    let mut case = test_case_read(test_case_file)?;

    info!("------ begin load data ------");
    test_btree_operations(
        btree.clone(),
        case.concurrent,
        case.init_kv,
        8192,
        8192,
        key_desc.tuple_oper().clone(),
        value_desc.tuple_oper().clone(),
    )
    .await?;

    info!("------ begin testing ------");

    let num_concurrent = case.concurrent;
    assert_ne!(num_concurrent, 0);
    let num_per_routine = case.operations.len() / num_concurrent as usize;
    let mut vec_ops = Vec::new();
    for _i in 0..num_concurrent {
        let len = case.operations.len();
        if len > num_per_routine {
            let ops = case.operations.split_off(len - num_per_routine);
            if !ops.is_empty() {
                vec_ops.push(ops);
            }
        }
    }
    if !case.operations.is_empty() {
        vec_ops.push(case.operations);
    }

    let thd = thread::spawn(move || {
        let local = LocalSet::new();
        for v in vec_ops {
            let key_d = key_desc.clone();
            let value_d = value_desc.clone();
            let bt = btree.clone();
            local.spawn_local(async move {
                test_btree_operations(
                    bt,
                    case.concurrent,
                    v,
                    8192,
                    8192,
                    key_d.tuple_oper().clone(),
                    value_d.tuple_oper().clone(),
                )
                .await
            });
        }

        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(local);
    });
    let r = thd.join();
    match r {
        Ok(()) => {
            info!("test btree done concurrent: {}", case.concurrent);
            Ok(())
        }
        Err(_e) => {
            return Err(ET::FatalError("join return error".to_string()));
        }
    }
}

async fn test_btree_async(
    buff_size: u64,
    page_size: u64,
    num_keys: u64,
    base_path: String,
    test_option: TestCaseOption<u64>,
) -> Result<()> {
    let schema_file_name = "schema_int_key.json".to_string();
    let test_input_file_path = match test_option {
        TestCaseOption::GenParam(n) => {
            info!("generate test case {} rows", num_keys);
            let csv_file_name = "test_input.csv".to_string();
            let test_case_file_name = "test_case_items.json".to_string();
            let mut path_buf1 = PathBuf::from(&base_path);
            path_buf1.push(csv_file_name.clone());
            let csv_path = path_buf1
                .as_path()
                .to_str()
                .unwrap_or(format!("/tmp/{}", csv_file_name).as_str())
                .to_string();
            let mut path_buf2 = PathBuf::from(&base_path);
            path_buf2.push(test_case_file_name.clone());
            let output_file = path_buf2
                .as_path()
                .to_str()
                .unwrap_or(format!("/tmp/{}", test_case_file_name).as_str())
                .to_string();
            create_test_csv_file(csv_path.clone(), num_keys)?;
            let concurrent = if n == 0 { 1 } else { n };
            generate_test_case(
                num_keys,
                output_file.clone(),
                csv_path,
                schema_file_name.clone(),
                n == 0,
                concurrent,
            )?;
            output_file
        }
        TestCaseOption::FromFile(file_name) => test_case_file_path(file_name)?,
    };

    info!("test case input file {}", test_input_file_path);

    test_btree_input_from_json(
        buff_size,
        page_size,
        base_path.clone(),
        test_input_file_path.clone(),
        schema_file_name.clone(),
    )
    .await?;
    Ok(())
}

const BUFF_SIZE: u64 = 1024 * 1024 * 1024;
const PAGE_SIZE: u64 = 1 * 512;

const NUM_KEYS: u64 = 10000;

fn test_btree(
    buff_size: u64,
    page_size: u64,
    num_keys: u64,
    base_path: String,
    option: TestCaseOption<u64>,
) {
    let local = LocalSet::new();
    local.spawn_local(async move {
        let r = test_btree_async(buff_size, page_size, num_keys, base_path, option)
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
}

#[async_trait]
impl tide::Endpoint<()> for Test {
    async fn call(&self, req: Request<()>) -> tide::Result {
        {
            let task = LocalSet::new();
            let this = self.clone();
            let _ = task.spawn_local(async move {
                let r = this.handle_req(req).await;
                assert!(r.is_ok());
            });
            let runtime = Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(task);
        }
        let ret = {
            let mut output = self.output.lock().await;
            let mut ret: String = String::new();
            std::mem::swap(&mut ret, &mut output);
            ret
        };
        Ok(ret.to_string().into())
    }
}

#[async_trait]
impl HandlePage for Test {
    async fn handle_non_leaf(&self, _parent: Option<PageId>, page: PageGuardEP) -> Result<()> {
        // self.fmt_page(page).await;
        let _ = self.handle_page(page).await;
        Ok(())
    }

    async fn handle_leaf(&self, _parent: Option<PageId>, page: PageGuardEP) -> Result<()> {
        // self.fmt_page(page).await;
        let _ = self.handle_page(page).await;
        Ok(())
    }
}

impl Test {
    async fn handle_req(&self, _req: Request<()>) -> Result<()> {
        info!("traversal");
        self.btree.tree_traversal(self).await?;
        info!("check_circle");
        self.check_circle().await;
        info!("check buffer pool");
        self.check_buffer_pool().await?;

        Ok(())
    }
    async fn check_circle(&self) {
        let mut guard = self.tx_wait.lock().await;
        let mut circle = HashSet::new();
        guard.detect_circle(&mut circle);
        for (i, vec) in circle.iter().enumerate() {
            error!("circle {}:", i);
            for id in vec {
                error!("       -> {}", id)
            }
        }
        guard.clear();
    }

    async fn check_buffer_pool(&self) -> Result<()> {
        let vec = self.pool.get_all_page().await;
        for g in &vec {
            let (wait, hold) = g.task_wait_for().await;
            info!("wait, {:?}, hold: {:?}", wait, hold);
            self.add_wait(wait, hold).await;
        }
        self.check_circle().await;
        for g in &vec {
            info!("{}", g.fmt().await?);
            g.unlock().await;
        }
        Ok(())
    }

    async fn handle_page(&self, page: PageGuardEP) -> Result<()> {
        let string = page.fmt().await?;
        let output = self.output.clone();
        let mut guard_output = output.lock().await;
        *guard_output += &*("\n".to_owned() + &string);

        let (wait, hold) = page.task_wait_for().await;
        self.add_wait(wait, hold).await;
        Ok(())
    }

    async fn add_wait(&self, wait: Vec<TaskId>, hold: Vec<TaskId>) {
        let wait_set = self.tx_wait.clone();
        let mut guard_wait_set = wait_set.lock().await;
        for i in &wait {
            for j in &hold {
                // convert TaskId to XID
                guard_wait_set.add(*j as XID, *i as XID);
            }
        }
    }
}
#[test]
fn test() {
    // console_subscriber::init();

    tracing_subscriber::fmt()
        //.pretty()
        .with_writer(std::sync::Mutex::new(
            File::create("test_btree.log").unwrap(),
        ))
        // enable info
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
    let tmp = dir.path().to_str().unwrap().to_string();
    for option in [
        TestCaseOption::FromFile("test_case_items_1.json".to_string()), // read from file
                                                                        // fixme, btree concurrency
                                                                        //TestCaseOption::FromFile("test_case_items_2.json".to_string()), // read from file
                                                                        //TestCaseOption::GenParam(0u64),                                 // random generate key value
                                                                        //TestCaseOption::GenParam(10u64),
                                                                        //TestCaseOption::GenParam(100u64), fixme, bugs when concurrency
    ] {
        test_btree(BUFF_SIZE, PAGE_SIZE, NUM_KEYS, tmp.clone(), option);
    }
}
