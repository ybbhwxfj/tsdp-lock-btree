use std::cmp::Ordering;
use std::marker::PhantomData;
use std::{mem, slice};

use json::JsonValue;
use rand::rngs::ThreadRng;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde::Serialize;

use adt::compare::Compare;
use adt::slice::Slice;
use common::result::Result;
use storage::access::to_json::ToJson;

pub trait SliceStub<T: Clone + DeserializeOwned + Serialize>: Slice + Send + Sync {
    fn from_slice(s: &[u8]) -> Self;
    fn from_type(t: &T) -> Self;
    fn to_type(&self) -> T;
    fn to_json(&self) -> JsonValue;
}

pub trait GenStub<T: Ord + Clone>: Default + Clone {
    fn gen(&mut self) -> T;
}

pub struct SliceToJson<
    KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync,
    KS: SliceStub<KT>,
> {
    phantom_kv: PhantomData<KT>,
    phantom_ks: PhantomData<KS>,
}

pub struct CompareStub<
    KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync,
    KS: SliceStub<KT>,
> {
    phantom_kv: PhantomData<KT>,
    phantom_ks: PhantomData<KS>,
}

impl<KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync, KS: SliceStub<KT>> Clone
    for SliceToJson<KT, KS>
{
    fn clone(&self) -> Self {
        Self {
            phantom_kv: Default::default(),
            phantom_ks: Default::default(),
        }
    }
}

impl<KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync, KS: SliceStub<KT>> ToJson
    for SliceToJson<KT, KS>
{
    fn to_json(&self, value: &[u8]) -> Result<JsonValue> {
        if value.is_empty() {
            Ok(JsonValue::Null)
        } else {
            let s = KS::from_slice(value);
            Ok(s.to_json())
        }
    }
}

impl<KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync, KS: SliceStub<KT>> Default
    for SliceToJson<KT, KS>
{
    fn default() -> Self {
        Self {
            phantom_kv: Default::default(),
            phantom_ks: Default::default(),
        }
    }
}

impl<KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync, KS: SliceStub<KT>> Clone
    for CompareStub<KT, KS>
{
    fn clone(&self) -> Self {
        Self {
            phantom_kv: Default::default(),
            phantom_ks: Default::default(),
        }
    }
}

impl<KT: Ord + Clone + DeserializeOwned + Serialize + Send + Sync, KS: SliceStub<KT>> Compare<[u8]>
    for CompareStub<KT, KS>
{
    fn compare(&self, k1: &[u8], k2: &[u8]) -> Ordering {
        let s1 = KS::from_slice(k1);
        let s2 = KS::from_slice(k2);
        s1.to_type().cmp(&s2.to_type())
    }
}

// integer ------------------

pub struct IntegerSlice {
    v: u64,
}

pub struct IntegerGen {
    range: (u64, u64),
    rnd: ThreadRng,
}

pub struct IntegerCompare {}

impl Default for IntegerCompare {
    fn default() -> Self {
        Self {}
    }
}

impl Clone for IntegerCompare {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl Compare<[u8]> for IntegerCompare {
    fn compare(&self, k1: &[u8], k2: &[u8]) -> Ordering {
        let s1 = IntegerSlice::from_slice(k1);
        let s2 = IntegerSlice::from_slice(k2);
        s1.v.cmp(&s2.v)
    }
}

impl Slice for IntegerSlice {
    fn as_slice(&self) -> &[u8] {
        unsafe {
            let p0: *const u64 = &self.v;
            let p: *const u8 = p0 as *const _;
            slice::from_raw_parts(p, mem::size_of::<i64>())
        }
    }
}

impl SliceStub<u64> for IntegerSlice {
    fn from_slice(s: &[u8]) -> Self {
        if s.len() != mem::size_of::<u64>() {
            panic!("error")
        }
        let v = unsafe {
            let mut v: u64 = 0;
            let p0: *mut u64 = &mut v;
            let p: *mut u8 = p0 as *mut _;
            p.copy_from(s.as_ptr(), s.len());
            v
        };
        Self { v }
    }

    fn from_type(v: &u64) -> Self {
        Self { v: v.clone() }
    }

    fn to_type(&self) -> u64 {
        self.v
    }

    fn to_json(&self) -> JsonValue {
        JsonValue::from(self.v)
    }
}

impl Default for IntegerGen {
    fn default() -> Self {
        IntegerGen::new((1000, 2000))
    }
}

impl Clone for IntegerGen {
    fn clone(&self) -> Self {
        Self {
            range: self.range.clone(),
            rnd: Default::default(),
        }
    }
}

impl GenStub<u64> for IntegerGen {
    fn gen(&mut self) -> u64 {
        self.rnd.gen_range(self.range.0..self.range.1)
    }
}

impl IntegerGen {
    pub fn new(range: (u64, u64)) -> Self {
        Self {
            range,
            rnd: ThreadRng::default(),
        }
    }
}

// string ------------------
pub struct StringSlice {
    v: String,
}

pub struct StringGen {
    max_size: u32,
    min_size: u32,
    vec: Vec<char>,
    rnd: ThreadRng,
}

impl Slice for StringSlice {
    fn as_slice(&self) -> &[u8] {
        self.v.as_bytes()
    }
}

impl SliceStub<String> for StringSlice {
    fn from_slice(s: &[u8]) -> Self {
        let v = std::str::from_utf8(s).unwrap();
        Self { v: v.to_string() }
    }

    fn from_type(v: &String) -> Self {
        Self { v: v.clone() }
    }

    fn to_type(&self) -> String {
        self.v.clone()
    }

    fn to_json(&self) -> JsonValue {
        JsonValue::from(self.v.clone())
    }
}

impl Clone for StringGen {
    fn clone(&self) -> Self {
        Self {
            max_size: self.max_size,
            min_size: self.min_size,
            vec: self.vec.clone(),
            rnd: Default::default(),
        }
    }
}

impl GenStub<String> for StringGen {
    fn gen(&mut self) -> String {
        let length = self.rnd.gen_range(self.min_size..self.max_size);
        Self::generate(length as usize, &self.vec, &mut self.rnd)
    }
}

impl StringGen {
    pub fn new<S: AsRef<str>>(charset: S, min_size: u32, max_size: u32) -> Self {
        let s = charset.as_ref();

        if s.is_empty() {
            panic!("charset is empty!");
        }

        let chars: Vec<char> = s.chars().collect();
        Self {
            max_size,
            min_size,
            vec: chars,
            rnd: ThreadRng::default(),
        }
    }
    fn generate(length: usize, chars: &Vec<char>, rnd: &mut ThreadRng) -> String {
        if chars.is_empty() {
            panic!("Provided charset is empty! It should contain at least one character");
        }

        let mut result = String::with_capacity(length);

        unsafe {
            for _ in 0..length {
                result.push(*chars.get_unchecked(rnd.gen_range(0..chars.len())));
            }
        }

        result
    }
}

impl Default for StringGen {
    fn default() -> Self {
        StringGen::new(
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
            10,
            20,
        )
    }
}
