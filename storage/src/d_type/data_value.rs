use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use common::result::Result;

use crate::d_type::datum_item::{ComparableItem, DatumItem};
use crate::d_type::double::DoubleItem;
use crate::d_type::fixchar::CharItem;
use crate::d_type::float::FloatItem;
use crate::d_type::integer::IntegerItem;
use crate::d_type::long::LongItem;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ItemValue {
    Integer(IntegerItem),

    Long(LongItem),

    Float(FloatItem),

    Double(DoubleItem),

    Char(CharItem),

    VarChar(CharItem),
}

macro_rules! value_invoke {
    ($x:ident, $func:ident, $($argv:expr), * )=> {
        match $x {
            ItemValue::Integer(reference) => { reference.$func($($argv), * ) },
            ItemValue::Long(reference) => { reference.$func($($argv), *) },
            ItemValue::Float(reference) => { reference.$func($($argv), *) },
            ItemValue::Double(reference) => { reference.$func($($argv), *) },
            ItemValue::Char(reference) => { reference.$func($($argv), *) },
            ItemValue::VarChar(reference) => { reference.$func($($argv), *) },
        }
    }
}

macro_rules! comparable_value_invoke {
    ($x:ident, $func:ident, $($argv:expr), * )=> {
        match $x {
            ItemValue::Integer(reference) => { reference.$func( $($argv), * ) },
            ItemValue::Long(reference) => { reference.$func( $($argv), *) },
            ItemValue::Char(reference) => { reference.$func( $($argv), *) },
            ItemValue::VarChar(reference) => { reference.$func( $($argv), *) },
            _ => { panic!("error") },
        }
    }
}

macro_rules! comparable_value_compare_invoke {
    ($x:ident, $y:ident, $func:ident, $($argv:expr), * )=> {
        match ($x, $y) {
            (ItemValue::Integer(reference1), ItemValue::Integer(reference2)) => {
                reference1.$func(reference2,  $($argv), * ) },
            (ItemValue::Long(reference1), ItemValue::Long(reference2)) => {
                reference1.$func(reference2, $($argv), *) },
            (ItemValue::Char(reference1), ItemValue::Char(reference2)) => {
                reference1.$func(reference2, $($argv), *) },
            (ItemValue::VarChar(reference1), ItemValue::VarChar(reference2)) => {
                reference1.$func(reference2, $($argv), *) },
            (_, _) => { panic!("error") }
        }
    }
}

impl DatumItem for ItemValue {
    fn init_from_slice(&mut self, slice: &[u8]) -> Result<()> {
        value_invoke!(self, init_from_slice, slice)
    }

    fn init_from_str(&mut self, string: &str) -> Result<()> {
        value_invoke!(self, init_from_str, string)
    }

    fn to_str(&self) -> Result<String> {
        value_invoke!(self, to_str,)
    }

    fn size(&self) -> u32 {
        value_invoke!(self, size,)
    }

    fn to_slice(&self, slice: &mut [u8]) -> Result<u32> {
        value_invoke!(self, to_slice, slice)
    }

    fn to_vec(&self) -> Vec<u8> {
        value_invoke!(self, to_vec,)
    }
}

impl Default for ItemValue {
    fn default() -> Self {
        Self::Integer(IntegerItem::new(0))
    }
}

impl Eq for ItemValue {}

impl PartialEq<Self> for ItemValue {
    fn eq(&self, other: &Self) -> bool {
        comparable_value_compare_invoke!(self, other, eq,)
    }
}

impl Hash for ItemValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        comparable_value_invoke!(self, hash, state);
    }
}

impl Ord for ItemValue {
    fn cmp(&self, other: &Self) -> Ordering {
        comparable_value_compare_invoke!(self, other, cmp,)
    }
}

impl PartialOrd<Self> for ItemValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        comparable_value_compare_invoke!(self, other, partial_cmp,)
    }
}

impl ComparableItem for ItemValue {}
