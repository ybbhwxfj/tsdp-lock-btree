use serde::{Deserialize, Serialize};

use common::id::OID;

use crate::d_type::data_value::ItemValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteOpType {
    Insert(Vec<u8>),
    Update(Vec<u8>),
    UpdateDelta(Vec<(u32, ItemValue)>),
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteOp {
    op_type: WriteOpType,
    table_id: OID,
    key: Vec<u8>,
}

impl WriteOp {
    pub fn new_insert(table_id: OID, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            op_type: WriteOpType::Insert(value),
            table_id,
            key,
        }
    }

    pub fn new_update(table_id: OID, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            op_type: WriteOpType::Update(value),
            table_id,
            key,
        }
    }

    pub fn new_delete(table_id: OID, key: Vec<u8>) -> Self {
        Self {
            op_type: WriteOpType::Delete,
            table_id,
            key,
        }
    }

    pub fn table_id(&self) -> OID {
        self.table_id
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn op_type(&self) -> &WriteOpType {
        &self.op_type
    }
}
