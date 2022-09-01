use std::cmp::Ordering;
use std::collections::HashMap;

use common::error_type::ET;

use common::result::Result;

use crate::access::const_val::TUPLE_HEADER_SIZE;
use crate::access::datum_desc::DatumDesc;

use crate::access::datum_oper;
use crate::access::schema::{ColumnSchema, TableSchema};
use crate::access::tuple_oper::TupleOper;
use crate::access::tuple_raw::TUPLE_SLOT_SIZE;
use crate::d_type::data_type::DataType;
use crate::d_type::data_type_func::DataTypeInit;

#[derive(Clone)]
pub struct TupleDesc {
    pub datum_desc: Vec<DatumDesc>,
    tuple_oper: TupleOper,
    slot_offset_begin: u32,
    slot_offset_end: u32,
    max_length: u32,
    fixed_count: u32,
    var_count: u32,
}

impl Default for TupleDesc {
    fn default() -> Self {
        Self::new()
    }
}

impl TupleDesc {
    pub fn new() -> TupleDesc {
        TupleDesc {
            datum_desc: vec![],
            tuple_oper: Default::default(),
            slot_offset_begin: 0,
            slot_offset_end: 0,
            max_length: 0,
            fixed_count: 0,
            var_count: 0,
        }
    }

    pub fn from_table_schema(
        s: &TableSchema,
        map: &HashMap<DataType, DataTypeInit>,
    ) -> Result<(TupleDesc, TupleDesc)> {
        let mut key_column = vec![];
        let mut value_column = vec![];

        for c in s.columns.iter() {
            if c.primary_key {
                key_column.push(c.clone());
            } else {
                value_column.push(c.clone())
            }
        }
        if key_column.len() == 0 || value_column.len() == 0 {
            panic!("do not support either all primary key or non primary key")
        }
        let key_desc = Self::column_schema_list_to_desc(&key_column, map)?;
        let value_desc = Self::column_schema_list_to_desc(&value_column, map)?;
        Ok((key_desc, value_desc))
    }

    pub fn datum_desc(&self) -> &Vec<DatumDesc> {
        &self.datum_desc
    }

    pub fn tuple_oper(&self) -> &TupleOper {
        &self.tuple_oper
    }

    pub fn slot_offset_end(&self) -> u32 {
        self.slot_offset_end
    }

    pub fn slot_offset_begin(&self) -> u32 {
        self.slot_offset_begin
    }

    pub fn max_size(&self) -> u32 {
        self.max_length
    }

    pub fn add_datum_desc(&mut self, d: DatumDesc) {
        self.datum_desc.push(d);
    }

    pub fn get_datum_desc(&self, index: usize) -> Option<&DatumDesc> {
        self.datum_desc.get(index)
    }

    fn column_schema_list_to_desc(
        s: &Vec<ColumnSchema>,
        map: &HashMap<DataType, DataTypeInit>,
    ) -> Result<TupleDesc> {
        let mut reorder = s.clone();
        let error_type: bool = false;

        reorder.sort_by(|x, y| {
            if x.fixed_length == y.fixed_length {
                // both fixed length or var length
                if x.data_type == y.data_type {
                    Ordering::Equal
                } else {
                    x.data_type.cmp(&y.data_type)
                }
            } else if x.fixed_length {
                Ordering::Less
            } else if y.fixed_length {
                Ordering::Greater
            } else {
                panic!("cannot go here")
            }
        });
        if error_type {
            return Err(ET::NoneOption);
        }
        let mut desc = TupleDesc::new();
        let mut offset: u32 = TUPLE_HEADER_SIZE;
        let mut slot_offset_begin: u32 = TUPLE_HEADER_SIZE;
        let mut slot_offset_end: u32 = TUPLE_HEADER_SIZE;
        let mut max_length: u32 = TUPLE_HEADER_SIZE;
        let mut fixed_count: u32 = 0;
        let mut fixed_length = true;
        let mut datum_ops = Vec::new();
        for (i, cs) in reorder.iter().enumerate() {
            let dt = match map.get(&cs.data_type) {
                Some(t) => t.clone(),
                None => {
                    return Err(ET::NoneOption);
                }
            };

            let op = datum_oper::DatumOper::from(&dt, cs.fixed_length, cs.max_length, offset);
            let dd = DatumDesc::new(
                cs.column_id,
                dt.data_type,
                dt.item_value_default.clone(),
                cs.primary_key,
                cs.not_null,
                cs.fixed_length,
                cs.max_length,
                cs.index,
                i as u32,
                offset,
                op.clone(),
            );

            if cs.fixed_length {
                offset += cs.max_length;
                max_length += cs.max_length;
                fixed_count += 1;
                slot_offset_end += cs.max_length;
            } else {
                if fixed_length {
                    fixed_length = fixed_length;
                    slot_offset_begin = offset;
                }
                offset = offset + TUPLE_SLOT_SIZE;
                slot_offset_end += TUPLE_SLOT_SIZE;
                max_length += cs.max_length + TUPLE_SLOT_SIZE;
            }

            datum_ops.push(op.clone());
            desc.add_datum_desc(dd);
        }

        desc.slot_offset_begin = slot_offset_begin;
        desc.slot_offset_end = slot_offset_end;
        desc.fixed_count = fixed_count;
        desc.var_count = reorder.len() as u32 - fixed_count;
        desc.max_length = max_length;
        desc.tuple_oper = TupleOper::new(slot_offset_begin, slot_offset_end, datum_ops);
        Ok(desc)
    }
}
