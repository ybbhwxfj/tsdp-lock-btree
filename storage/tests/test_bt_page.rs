#![feature(map_first_last)]

use json::JsonValue;
use log::trace;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cmp::Ordering;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::option::Option;

use std::{mem, slice};

use adt::compare::Compare;
use adt::slice::{FromSlice, Slice};
use common::result::Result;

use storage::access::const_val::INVALID_PAGE_ID;

use storage::access::modify_list::ModifyList;
use storage::access::page_bt::{PageBTree, PageBTreeMut, LEAF_PAGE, NON_LEAF_PAGE};
use storage::access::page_id::PageId;

use storage::access::to_json::{SelfToJson, ToJson};

use storage::access::upsert::{ResultUpsert, Upsert};

pub mod test_storage;

struct TestPage<
    K: Clone + Ord + Hash + Slice + FromSlice + Send + SelfToJson,
    V: Clone + Slice + FromSlice + Send + SelfToJson,
    GK: Clone + Fn(&K, &K, &mut ThreadRng) -> K,
    GV: Clone + Fn(&mut ThreadRng) -> V,
> {
    rnd: ThreadRng,
    max_key: K,
    min_key: K,
    key_set: BTreeMap<K, Option<PageId>>,
    next_page_id: PageId,
    fn_gen_key: GK,
    fn_gen_value: GV,
    compare: StubKeyCompare<K>,
    key2json: StubToJson<K>,
    value2json: StubToJson<V>,
}

#[derive(Clone)]
struct StubKeyCompare<K: Clone + Ord + Hash + Slice + FromSlice + Send> {
    _phantom: PhantomData<K>,
}

impl<K: Clone + Ord + Hash + Slice + FromSlice + Send> StubKeyCompare<K> {
    pub fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

impl<K: Clone + Ord + Hash + Slice + FromSlice + Send> Compare<[u8]> for StubKeyCompare<K> {
    fn compare(&self, k1: &[u8], k2: &[u8]) -> Ordering {
        if k1.is_empty() && k2.is_empty() {
            Ordering::Equal
        } else if k1.is_empty() {
            Ordering::Greater
        } else if k2.is_empty() {
            Ordering::Less
        } else {
            let kk1 = K::from_slice(k1);
            let kk2 = K::from_slice(k2);
            kk1.cmp(&kk2)
        }
    }
}

unsafe impl<K: Clone + Ord + Hash + Slice + FromSlice + Send> Send for StubKeyCompare<K> {}

unsafe impl<K: Clone + Ord + Hash + Slice + FromSlice + Send> Sync for StubKeyCompare<K> {}

#[derive(Clone)]
struct StubToJson<K: Clone + Slice + FromSlice + Send + SelfToJson> {
    _phantom: PhantomData<K>,
}

impl<K: Clone + Slice + FromSlice + Send + SelfToJson> StubToJson<K> {
    pub fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}
impl<K: Clone + Slice + FromSlice + Send + SelfToJson> ToJson for StubToJson<K> {
    fn to_json(&self, value: &[u8]) -> Result<JsonValue> {
        Ok(K::from_slice(value).to_json()?)
    }
}

unsafe impl<K: Clone + Slice + FromSlice + Send + SelfToJson> Send for StubToJson<K> {}

unsafe impl<K: Clone + Slice + FromSlice + Send + SelfToJson> Sync for StubToJson<K> {}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct IntegerKey {
    k: i32,
}

impl IntegerKey {
    fn new(k: i32) -> Self {
        Self { k }
    }
    fn value(&self) -> i32 {
        self.k
    }
}

impl Slice for IntegerKey {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p0: *const i32 = &self.k;
            let p: *const u8 = p0 as *const _;
            slice::from_raw_parts(p, mem::size_of::<i32>())
        }
    }
}

impl SelfToJson for IntegerKey {
    fn to_json(&self) -> Result<JsonValue> {
        Ok(JsonValue::from(self.k))
    }
}

impl FromSlice for IntegerKey {
    fn from_slice(s: &[u8]) -> Self {
        if s.len() != mem::size_of::<i32>() {
            panic!("error")
        }
        let k = unsafe {
            let mut v: i32 = 0;
            let p0: *mut i32 = &mut v;
            let p: *mut u8 = p0 as *mut _;
            p.copy_from(s.as_ptr(), s.len());
            v
        };
        Self { k }
    }
}

impl<
        K: Clone + Ord + Hash + Slice + FromSlice + Send + SelfToJson,
        V: Clone + Slice + FromSlice + Send + SelfToJson,
        GK: Clone + Fn(&K, &K, &mut ThreadRng) -> K,
        GV: Clone + Fn(&mut ThreadRng) -> V,
    > TestPage<K, V, GK, GV>
{
    fn new(
        min_key: K,
        max_key: K,
        gen_key: GK,
        gen_value: GV,
        compare: StubKeyCompare<K>,
        key2json: StubToJson<K>,
        value2json: StubToJson<V>,
    ) -> Self {
        Self {
            rnd: Default::default(),
            max_key,
            min_key,
            key_set: Default::default(),
            next_page_id: 0,

            fn_gen_key: gen_key,
            fn_gen_value: gen_value,
            compare,
            key2json,
            value2json,
        }
    }

    fn next_page_id(&mut self) -> PageId {
        self.next_page_id += 1;
        self.next_page_id
    }

    fn gen_value(&mut self) -> V {
        (self.fn_gen_value)(&mut self.rnd)
    }

    fn gen_key_between_range(&mut self) -> K {
        (self.fn_gen_key)(&self.min_key, &self.max_key, &mut self.rnd)
    }

    fn gen_exist(&mut self) -> K {
        if self.key_set.len() <= 2 {
            panic!("error");
        }
        let mut vec = Vec::new();
        for (k, _) in &self.key_set {
            vec.push(k.clone());
        }
        loop {
            let opt_key = vec.choose(&mut self.rnd);
            match opt_key {
                Some(key) => {
                    if *key != self.max_key && *key != self.min_key {
                        return key.clone();
                    }
                }
                None => {
                    panic!("error")
                }
            }
        }
    }

    fn gen_non_exist(&mut self, add_to_set: bool) -> K {
        loop {
            let key = self.gen_key_between_range();
            if !self.key_set.contains_key(&key) {
                if add_to_set {
                    let ok = self.key_set.insert(key.clone(), None);
                    assert!(ok.is_none());
                }
                return key.clone();
            }
        }
    }

    fn gen_max_min_leaf(&mut self) -> ((K, V), (K, V)) {
        let hk = self.max_key.clone();
        let lk = self.min_key.clone();

        let hv = self.gen_value();

        let _ = self.key_set.insert(hk.clone(), None);
        let lv = self.gen_value();

        let _ = self.key_set.insert(lk.clone(), None);
        ((lk.clone(), hv.clone()), (hk.clone(), lv.clone()))
    }

    fn gen_max_min_non_leaf(&mut self) -> ((K, PageId), (K, PageId)) {
        let lk = self.min_key.clone();
        let hk = self.max_key.clone();

        let lv = self.next_page_id();
        let _ = self.key_set.insert(lk.clone(), Some(lv));

        let hv = self.next_page_id();
        let _ = self.key_set.insert(hk.clone(), Some(hv));

        ((lk.clone(), lv.clone()), (hk.clone(), hv.clone()))
    }

    fn test_insert_non_leaf(
        &self,
        page: &mut Vec<u8>,
        new_key: &K,
        page_id: PageId,
        right_page: PageId,
        result: ResultUpsert,
        list: &mut ModifyList,
    ) -> Result<()> {
        let mut page_mut = PageBTreeMut::<NON_LEAF_PAGE>::new(page);
        let inf_high_key = Vec::new();
        trace!(
            "page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );

        let r = page_mut.non_leaf_search_and_insert_child(
            new_key,
            &inf_high_key,
            page_id,
            right_page,
            &self.compare,
            list,
        )?;
        trace!(
            "page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );
        assert_eq!(result, r);

        Ok(())
    }

    fn test_update_non_leaf(
        &self,
        page: &mut Vec<u8>,
        old_key: &K,
        new_key: &K,
        page_id: PageId,
        result: ResultUpsert,
        list: &mut ModifyList,
    ) -> Result<()> {
        let mut page_mut = PageBTreeMut::<NON_LEAF_PAGE>::new(page);

        trace!(
            "test_update_non_leaf, page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );

        let r = page_mut.non_leaf_search_and_update_key(
            new_key,
            old_key,
            page_id,
            &self.compare,
            list,
        )?;
        trace!(
            "test_update_non_leaf, page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );
        assert_eq!(result, r);

        Ok(())
    }

    fn test_upsert_leaf(
        &self,
        page: &mut Vec<u8>,
        key: &K,
        value: &V,
        upsert: Upsert,
        result: ResultUpsert,
    ) -> Result<()> {
        let mut page_mut = PageBTreeMut::<LEAF_PAGE>::new(page);
        trace!(
            "page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );

        let mut list = ModifyList::new();
        let r = page_mut.leaf_search_upsert(
            key,
            value,
            upsert,
            INVALID_PAGE_ID,
            &self.compare,
            &mut list,
            &mut None,
        )?;

        trace!(
            "page, {}",
            page_mut
                .to_json(&self.key2json, &self.value2json)?
                .to_string()
        );
        assert_eq!(r, result);
        Ok(())
    }

    fn gen_non_exist_key_id(&mut self) -> (K, PageId) {
        loop {
            let key = self.gen_key_between_range();
            if !self.key_set.contains_key(&key) {
                let id = self.next_page_id();
                let ok = self.key_set.insert(key.clone(), Some(id));
                assert!(ok.is_none());

                return (key.clone(), id);
            }
        }
    }

    fn gen_update_key_id(&mut self, less_than_next: bool) -> (K, K, PageId) {
        let mut key_page_id = Vec::new();

        for (k, v) in self.key_set.iter() {
            key_page_id.push((k.clone(), v.unwrap()));
        }

        if key_page_id.len() < 3 {
            panic!("error")
        }
        loop {
            let n: usize = self.rnd.gen_range(0..(key_page_id.len() - 3));
            let (k1, id1) = &key_page_id[n];
            let (k2, _id2) = &key_page_id[n + 1];
            let (k3, _id3) = &key_page_id[n + 2];
            let new_key = if less_than_next {
                (self.fn_gen_key)(k1, k2, &mut self.rnd)
            } else {
                (self.fn_gen_key)(k2, k3, &mut self.rnd)
            };
            if !self.key_set.contains_key(&new_key) {
                let opt = self.key_set.remove(k1);
                assert!(opt.is_some());
                self.key_set.insert(new_key.clone(), Some(id1.clone()));
                return (k1.clone(), new_key.clone(), id1.clone());
            }
        }
    }

    fn test_page_upsert_non_leaf(
        &mut self,
        page_size: u32,
        init_rows: u32,
        test_op_count: u32,
    ) -> Result<()> {
        let id = self.next_page_id();
        let mut page = create_page(
            page_size as usize,
            id,
            INVALID_PAGE_ID,
            INVALID_PAGE_ID,
            1, // not 0 for non leaf
        )?;
        self.insert_max_min::<NON_LEAF_PAGE>(&mut page)?;
        for _i in 0..init_rows {
            let (key, id) = self.gen_non_exist_key_id();
            let mut list = ModifyList::new();
            self.test_insert_non_leaf(
                &mut page,
                &key,
                id,
                INVALID_PAGE_ID,
                ResultUpsert::UpsertOk,
                &mut list,
            )?;
            self.check_page::<NON_LEAF_PAGE>(&page);
        }

        for _i in 0..test_op_count {
            let mut list = ModifyList::new();
            {
                self.check_page::<NON_LEAF_PAGE>(&page);

                let (old_k, new_k, id) = self.gen_update_key_id(true);

                let _ = self.test_update_non_leaf(
                    &mut page,
                    &old_k,
                    &new_k,
                    id,
                    ResultUpsert::UpsertOk,
                    &mut list,
                )?;

                self.check_page::<NON_LEAF_PAGE>(&page);
            }
            /*{
                let (old_k, new_k, id) = self.gen_exist_key_id(false);
                let r = self.test_update_non_leaf(
                    &mut page, &old_k, &new_k,
                    id, ResultUpsert::UpsertOk, &mut list);
                assert!(r.is_err());
                assert_eq!(self.map_leaf.len() + 1, PageBTree::<NON_LEAF_PAGE>::from(&page).get_count() as usize);
            }*/
            {
                let (key, id) = self.gen_non_exist_key_id();
                assert!(self.key_set.contains_key(&key));
                self.test_insert_non_leaf(
                    &mut page,
                    &key,
                    id,
                    INVALID_PAGE_ID,
                    ResultUpsert::UpsertOk,
                    &mut list,
                )?;
                self.check_page::<NON_LEAF_PAGE>(&page);
            }

            {
                let key = self.gen_exist();
                assert!(self.key_set.contains_key(&key));
                let id = self.next_page_id();
                self.test_insert_non_leaf(
                    &mut page,
                    &key,
                    id,
                    INVALID_PAGE_ID,
                    ResultUpsert::ExistingKey,
                    &mut list,
                )?;
                self.check_page::<NON_LEAF_PAGE>(&page);
            }
        }
        Ok(())
    }

    // we assume [count] and the page size are proper to pass the testing
    fn test_page_upsert_leaf(&mut self, page_size: u32, init_rows: u32, count: u32) -> Result<()> {
        let id = self.next_page_id();
        let mut page = create_page(page_size as usize, id, INVALID_PAGE_ID, INVALID_PAGE_ID, 0)?;
        self.insert_max_min::<LEAF_PAGE>(&mut page)?;
        for _i in 0..init_rows {
            let key = self.gen_non_exist(true);
            let value = self.gen_value();
            self.test_upsert_leaf(
                &mut page,
                &key,
                &value,
                Upsert::Insert,
                ResultUpsert::UpsertOk,
            )?;
        }
        for _i in 0..count {
            {
                let key = self.gen_non_exist(false);
                let value = self.gen_value();
                self.test_upsert_leaf(
                    &mut page,
                    &key,
                    &value,
                    Upsert::Update,
                    ResultUpsert::NonExistingKey,
                )?;
            }
            {
                let key = self.gen_exist();
                assert!(self.key_set.contains_key(&key));

                let value = self.gen_value();
                self.test_upsert_leaf(
                    &mut page,
                    &key,
                    &value,
                    Upsert::Update,
                    ResultUpsert::UpsertOk,
                )?;
            }

            {
                let key = self.gen_non_exist(true);
                let value = self.gen_value();
                self.test_upsert_leaf(
                    &mut page,
                    &key,
                    &value,
                    Upsert::Insert,
                    ResultUpsert::UpsertOk,
                )?;
            }
            {
                let key = self.gen_exist();
                let value = self.gen_value();
                self.test_upsert_leaf(
                    &mut page,
                    &key,
                    &value,
                    Upsert::Insert,
                    ResultUpsert::ExistingKey,
                )?;
            }
        }
        Ok(())
    }

    fn insert_max_min<const IS_LEAF: bool>(&mut self, vec: &mut Vec<u8>) -> Result<()> {
        let mut list = ModifyList::new();
        if IS_LEAF {
            let ((lk, lv), (hk, hv)) = self.gen_max_min_leaf();
            let mut page_left = PageBTreeMut::<IS_LEAF>::new(vec);

            for (k, v) in [(lk, lv), (hk, hv)] {
                let _r = page_left.leaf_search_upsert(
                    &k,
                    &v,
                    Upsert::Insert,
                    INVALID_PAGE_ID,
                    &self.compare,
                    &mut list,
                    &mut None,
                )?;
            }
        } else {
            let ((lk, lv), (hk, hv)) = self.gen_max_min_non_leaf();
            let mut page_left = PageBTreeMut::<IS_LEAF>::new(vec);
            let high_key = Vec::new();
            for (k, v) in [(lk, lv), (hk, hv)] {
                let r = page_left.non_leaf_search_and_insert_child(
                    &k,
                    &high_key,
                    v,
                    INVALID_PAGE_ID,
                    &self.compare,
                    &mut list,
                )?;
                assert_eq!(r, ResultUpsert::UpsertOk)
            }
        }

        Ok(())
    }

    fn check_page<const IS_LEAF: bool>(&self, vec: &Vec<u8>) {
        let page = PageBTree::<IS_LEAF>::from(vec);
        page.check_invariant(&self.compare);
        let count = page.get_count();
        // inf high key in page +1
        assert_eq!(count as usize, self.key_set.len() + 1);
        for i in 0..count {
            let key = page.get_key(i);
            if !key.is_empty() {
                let k = K::from_slice(key);
                let has_key = self.key_set.contains_key(&k);
                assert!(has_key);
            }
        }
    }

    fn test_page_split<const IS_LEAF: bool>(&mut self, page_size: u32) -> Result<()> {
        let level = if IS_LEAF { 0 } else { 1 };
        let cmp = StubKeyCompare::<K>::new();
        let left_id = self.next_page_id();
        let right_id = self.next_page_id();
        let mut left = create_page(
            page_size as usize,
            left_id,
            INVALID_PAGE_ID,
            INVALID_PAGE_ID,
            level,
        )?;
        let mut right = create_page(
            page_size as usize,
            right_id,
            INVALID_PAGE_ID,
            INVALID_PAGE_ID,
            level,
        )?;
        let mut list = ModifyList::new();
        self.insert_max_min::<IS_LEAF>(&mut left)?;
        let mut opt_split: Option<u32> = None;
        while opt_split.is_none() {
            let mut page_left = PageBTreeMut::<IS_LEAF>::new(&mut left);
            if IS_LEAF {
                let (k, v) = (self.gen_non_exist(true), self.gen_value());
                let r = page_left.leaf_search_upsert(
                    &k,
                    &v,
                    Upsert::Insert,
                    INVALID_PAGE_ID,
                    &cmp,
                    &mut list,
                    &mut None,
                )?;
                match r {
                    ResultUpsert::UpsertOk => {}
                    ResultUpsert::NeedSplit(i) => {
                        opt_split = Some(i);
                    }
                    _ => {
                        panic!("not possible")
                    }
                }
            } else {
                let (k, page_id) = (self.gen_key_between_range(), self.next_page_id());
                let inf_key: Vec<u8> = Vec::new();
                let r = page_left.non_leaf_search_and_insert_child(
                    &k,
                    &inf_key,
                    page_id,
                    INVALID_PAGE_ID,
                    &cmp,
                    &mut list,
                )?;
                match r {
                    ResultUpsert::UpsertOk => {}
                    ResultUpsert::NeedSplit(i) => {
                        opt_split = Some(i);
                    }
                    _ => {
                        panic!("not possible")
                    }
                }
            }
        }

        match opt_split {
            Some(index) => {
                let mut page_left = PageBTreeMut::<IS_LEAF>::new(&mut left);
                let space = page_left.get_free_space();
                let (split_pos, _) =
                    page_left.re_organize_and_split_position::<IS_LEAF>(index, space)?;

                page_left.split(&mut right, split_pos);
            }
            _ => {}
        }
        // final update link pointer
        {
            let mut page_left_mut = PageBTreeMut::<IS_LEAF>::new(&mut left);
            let mut page_right_mut = PageBTreeMut::<IS_LEAF>::new(&mut right);
            page_left_mut.set_right_id(page_right_mut.get_id());
            page_right_mut.set_left_id(page_left_mut.get_id());
        }
        {
            let page_left = PageBTree::<IS_LEAF>::from(&left);
            let page_right = PageBTree::<IS_LEAF>::from(&right);
            page_left.check_invariant(&cmp);
            page_right.check_invariant(&cmp);

            // page_left.check_left_lt_right(&page_right, &cmp, &k2j, &v2j);
            //assert!(kv_map.is_empty());
        }
        Ok(())
    }
}

fn create_page(
    page_size: usize,
    id: PageId,
    left_id: PageId,
    right_id: PageId,
    level: u32,
) -> Result<Vec<u8>> {
    let mut vec: Vec<u8> = vec![];
    vec.resize(page_size, 0);
    if level == 0 {
        let mut page = PageBTreeMut::<LEAF_PAGE>::new(&mut vec);
        page.format(id, left_id, right_id, level);
        if right_id == INVALID_PAGE_ID {
            let _ = page.set_high_key_infinite();
        }
    } else {
        let mut page = PageBTreeMut::<NON_LEAF_PAGE>::new(&mut vec);
        page.format(id, left_id, right_id, level);
        if right_id == INVALID_PAGE_ID {
            let _ = page.set_high_key_infinite();
        }
    }
    Ok(vec)
}

fn test_all() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        // display source code file paths
        .with_file(true)
        // display source code line numbers
        .with_line_number(true)
        // disable targets
        .with_target(false)
        // sets this to be the default, global collector for this application.
        .init();
    let _ = env_logger::try_init();
    trace!("TESTING bt_page");

    let gen_key = |k1: &IntegerKey, k2: &IntegerKey, rnd: &mut ThreadRng| -> IntegerKey {
        IntegerKey::new(rnd.gen_range(k1.value()..k2.value()))
    };
    let gen_value = |rnd: &mut ThreadRng| -> IntegerKey { IntegerKey::new(rnd.gen()) };

    for is_leaf in [true, false] {
        let mut test = TestPage::new(
            IntegerKey::new(0),
            IntegerKey::new(100000),
            gen_key,
            gen_value,
            StubKeyCompare::<IntegerKey>::new(),
            StubToJson::<IntegerKey>::new(),
            StubToJson::<IntegerKey>::new(),
        );
        let page_size = 1024 as u32;
        if is_leaf {
            test.test_page_split::<true>(page_size)?
        } else {
            test.test_page_split::<false>(page_size)?
        };
    }

    for is_leaf in [true, false] {
        let mut test = TestPage::new(
            IntegerKey::new(0),
            IntegerKey::new(100000),
            gen_key,
            gen_value,
            StubKeyCompare::<IntegerKey>::new(),
            StubToJson::<IntegerKey>::new(),
            StubToJson::<IntegerKey>::new(),
        );
        if is_leaf {
            test.test_page_upsert_leaf(1024 * 10, 100, 100)?
        } else {
            test.test_page_upsert_non_leaf(1024 * 10, 100, 100)?;
        };
    }
    Ok(())
}

#[test]
fn bt_page_test() {
    let r = test_all();
    match r {
        Ok(()) => {}
        Err(e) => {
            trace!("{}", e.to_string())
        }
    }
}
