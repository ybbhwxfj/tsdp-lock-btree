use std::cmp::Ordering;

use json::{object, JsonValue};

use adt::compare::Compare;
use common::error_type::ET;

use crate::access::datum::Datum;

use crate::access::tuple_oper::TupleOper;
use crate::access::tuple_raw::TupleRaw;

pub struct BtKeyRaw<'p> {
    slice: &'p [u8],
}

impl<'p> Clone for BtKeyRaw<'p> {
    fn clone(&self) -> Self {
        BtKeyRaw::from_slice(self.slice)
    }
}

impl<'p> BtKeyRaw<'p> {
    pub fn cmp(&self, other: &Self, desc: &TupleOper) -> Ordering {
        self.cmp_gut(other, desc)
    }

    pub fn from_slice(s: &'p [u8]) -> BtKeyRaw<'p> {
        BtKeyRaw { slice: s }
    }

    pub fn len(&self) -> u32 {
        self.slice.len() as u32
    }

    pub fn slice(&self) -> &'p [u8] {
        self.slice
    }

    pub fn to_json(&self, desc: &TupleOper) -> Result<JsonValue, ET> {
        if self.is_empty() {
            return Ok(object! {
                "key": "null",
            });
        }
        let tuple = TupleRaw::from_slice(self.slice);
        tuple.to_json(desc)
    }

    pub fn compare<'a>(k1: &BtKeyRaw<'a>, k2: &BtKeyRaw<'a>, key_desc: &TupleOper) -> Ordering {
        let k1_inf = k1.is_empty();
        let k2_inf = k2.is_empty();
        if k1_inf && k2_inf {
            Ordering::Equal
        } else if k1_inf {
            Ordering::Greater
        } else if k2_inf {
            Ordering::Less
        } else {
            k1.cmp(&k2, &key_desc)
        }
    }

    /// is high key infinite(maximum key)
    pub fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    fn cmp_gut(&'p self, other: &'p Self, desc: &TupleOper) -> Ordering {
        for d in desc.datum_oper().iter() {
            let d1: Datum<'p> = Datum::new(d.tuple_get_datum(&self.slice));
            let d2: Datum<'p> = Datum::new(d.tuple_get_datum(other.slice));
            let ord: Ordering = d.datum_compare(d1.slice(), d2.slice());
            match ord {
                Ordering::Equal => {
                    continue;
                }
                _ => {
                    return ord;
                }
            }
        }
        Ordering::Equal
    }
}

#[derive(Clone)]
pub struct BtKeyRawCmp {
    desc: TupleOper,
}

impl BtKeyRawCmp {
    pub fn new(key_desc: TupleOper) -> BtKeyRawCmp {
        BtKeyRawCmp { desc: key_desc }
    }
}

impl Compare<[u8]> for BtKeyRawCmp {
    fn compare(&self, k1: &[u8], k2: &[u8]) -> Ordering {
        let k1 = BtKeyRaw::from_slice(k1);
        let k2 = BtKeyRaw::from_slice(k2);
        BtKeyRaw::compare(&k1, &k2, &self.desc)
    }
}
