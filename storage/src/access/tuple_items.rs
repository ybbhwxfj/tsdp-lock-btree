use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use common::result::Result;

use crate::access::datum_oper::DatumOper;

use crate::access::tuple_oper::TupleOper;
use crate::access::tuple_raw::TupleRaw;
use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TupleItems {
    items: Vec<ItemValue>,
}

impl TupleItems {
    pub fn new() -> Self {
        TupleItems { items: vec![] }
    }

    pub fn from_items(items: Vec<ItemValue>) -> Self {
        TupleItems { items }
    }

    pub fn add_items(&mut self, i: ItemValue) {
        self.items.push(i);
    }

    pub fn items(&self) -> &Vec<ItemValue> {
        &self.items
    }

    pub fn from_slice(slice: &[u8], desc: &TupleOper) -> Result<Self> {
        let mut items = vec![];
        let tuple = TupleRaw::from_slice(slice);
        for d in desc.datum_oper().iter() {
            let datum = tuple.get_datum(d);
            let item = datum.to_item(d)?;
            items.push(item);
        }
        Ok(TupleItems { items })
    }

    /// reformat the item value with the
    ///
    pub fn reformat_raw_tuple(&self, ops: &Vec<DatumOper>) -> Result<(Vec<u8>, Vec<DatumOper>)> {
        if ops.len() != self.items.len() {
            panic!("error");
        }
        self.reformat_fixed_length_raw_tuple(ops)
    }

    fn reformat_fixed_length_raw_tuple(
        &self,
        ops: &Vec<DatumOper>,
    ) -> Result<(Vec<u8>, Vec<DatumOper>)> {
        let mut vec = Vec::new();
        let mut tuple = Vec::new();
        let mut offset = 0;
        for (i, item) in self.items.iter().enumerate() {
            let mut op = (&ops[i]).clone();
            let size = item.size();
            offset += size;
            op.set_max_length(size);
            op.set_offset(offset);
            op.set_fixed_length(true);
            vec.push(op.clone())
        }
        tuple.resize(offset as usize, 0);

        let mut total_size = 0;
        for (i, item) in self.items.iter().enumerate() {
            let op = &(vec[i]);
            let n = op.tuple_set_datum(tuple.as_mut_slice(), item)?;
            total_size += n;
        }
        assert_eq!(total_size, offset);
        Ok((tuple, vec))
    }
}

impl Hash for TupleItems {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for i in self.items.iter() {
            i.hash(state);
        }
    }
}

impl PartialEq<Self> for TupleItems {
    fn eq(&self, other: &Self) -> bool {
        self.items.eq(&other.items)
    }
}

impl Eq for TupleItems {}

impl PartialOrd<Self> for TupleItems {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return if self.items.len() != other.items.len() {
            None
        } else {
            for (i, item) in self.items.iter().enumerate() {
                let other_item = unsafe { other.items.get_unchecked(i) };
                let n = item.cmp(other_item);
                if !n.is_eq() {
                    return Some(n);
                }
            }
            Some(Ordering::Equal)
        };
    }
}

impl Ord for TupleItems {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.items.len() != other.items.len() {
            panic!("cannot compare");
        }

        for (i, item) in self.items.iter().enumerate() {
            let other_item = unsafe { other.items.get_unchecked(i) };
            let n = item.cmp(other_item);
            if !n.is_eq() {
                return n;
            }
        }
        Ordering::Equal
    }
}
