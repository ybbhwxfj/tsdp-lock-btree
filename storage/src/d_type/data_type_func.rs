use std::collections::HashMap;

use crate::d_type::data_type::DataType;
use crate::d_type::data_type_init;
use crate::d_type::data_value::ItemValue;
use crate::d_type::fix_len::{fn_get_datum, fn_set_datum};
use crate::d_type::fn_datum::{
    FnCompare, FnDefaultItem, FnGetDatum, FnGetSlot, FnSetDatum, FnSetSlot,
};
use crate::d_type::var_len::{fn_get_slot, fn_set_slot};

pub const DT_INVALID: u32 = 0;
pub const DT_I32: u32 = DataType::Integer as u32;
pub const DT_I64: u32 = DataType::Long as u32;
pub const DT_F32: u32 = DataType::Float as u32;
pub const DT_F64: u32 = DataType::Double as u32;
pub const DT_VARCHAR: u32 = DataType::VarChar as u32;
pub const DT_CHAR: u32 = DataType::Char as u32;

#[derive(Clone, Copy)]
pub struct DatumFunc {
    pub fn_compare: FnCompare,
    pub fn_get_datum: FnGetDatum,
    pub fn_set_datum: FnSetDatum,
    pub fn_get_slot: FnGetSlot,
    pub fn_set_slot: FnSetSlot,
}

#[derive(Clone)]
pub struct DataTypeInit {
    pub data_type: DataType,
    pub item_value_default: ItemValue,

    pub fn_default: FnDefaultItem,

    pub fn_datum: DatumFunc,
}

impl DataTypeInit {
    pub fn new(
        data_type: DataType,
        item_value_default: ItemValue,
        fn_compare: FnCompare,
        fn_default: FnDefaultItem,
    ) -> DataTypeInit {
        DataTypeInit {
            data_type,

            item_value_default,

            fn_default,

            fn_datum: DatumFunc {
                fn_compare,
                fn_get_datum,
                fn_set_datum,
                fn_get_slot,
                fn_set_slot,
            },
        }
    }
}

pub fn map_name_to_data_type() -> HashMap<DataType, DataTypeInit> {
    let mut name2dt: HashMap<DataType, DataTypeInit> = HashMap::new();
    let vec = data_type_init::mapping_data_type();
    for dt in vec {
        let opt = name2dt.insert(dt.data_type.clone(), dt);
        match opt {
            Some(_) => {
                panic!("can happen");
            }
            _ => {}
        }
    }
    name2dt
}
