use serde::{Deserialize, Serialize};

use common::id::{gen_oid, OID};

use crate::d_type::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_id: OID,
    pub index_id: OID,
    pub heap_id: OID,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub column_id: OID,
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub not_null: bool,
    pub fixed_length: bool,
    pub max_length: u32,
    pub index: u32,
}

impl TableSchema {
    pub fn new(name: String) -> TableSchema {
        TableSchema {
            table_id: gen_oid(),
            index_id: gen_oid(),
            heap_id: gen_oid(),
            name,
            columns: vec![],
        }
    }

    pub fn add_column(&mut self, column_schema: ColumnSchema) {
        let mut column = column_schema;
        let index = self.columns.len() as u32;
        column.index = index;
        self.columns.push(column);
    }
}

impl ColumnSchema {
    pub fn new1(name: String) -> Self {
        Self {
            column_id: gen_oid(),
            name,
            data_type: DataType::Integer,
            primary_key: false,
            not_null: false,
            fixed_length: false,
            max_length: 0,
            index: 0,
        }
    }
    pub fn new(
        name: String,
        data_type: DataType,
        primary_key: bool,
        not_null: bool,
        fixed_length: bool,
        max_length: u32,
    ) -> ColumnSchema {
        ColumnSchema {
            column_id: gen_oid(),
            name,
            data_type,
            primary_key,
            not_null,
            fixed_length,
            max_length,
            index: 0,
        }
    }
}
