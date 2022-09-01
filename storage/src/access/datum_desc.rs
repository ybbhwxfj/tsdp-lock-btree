use common::id::OID;

use crate::access::datum_oper::DatumOper;

use crate::d_type::data_type::DataType;
use crate::d_type::data_value::ItemValue;

#[derive(Clone)]
pub struct DatumDesc {
    oid: OID,
    data_type: DataType,
    item_value: ItemValue,
    primary_key: bool,
    not_null: bool,
    fixed_length: bool,
    max_length: u32,
    // original index defined in table schema
    original_index: u32,
    datum_index: u32,
    offset: u32,

    datum_oper: DatumOper,
}

impl DatumDesc {
    pub fn new(
        oid: OID,
        data_type: DataType,
        item_value: ItemValue,
        primary_key: bool,
        not_null: bool,
        fixed_length: bool,
        max_length: u32,
        original_index: u32,
        datum_index: u32,
        offset: u32,
        datum_oper: DatumOper,
    ) -> DatumDesc {
        DatumDesc {
            oid,
            data_type,
            item_value,
            primary_key,
            not_null,
            fixed_length,
            max_length,
            original_index,
            datum_index,
            offset,

            datum_oper,
        }
    }

    pub fn column_id(&self) -> OID {
        self.oid
    }

    pub fn column_data_type(&self) -> DataType {
        self.data_type
    }

    pub fn column_default_value(&self) -> ItemValue {
        self.item_value.clone()
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    pub fn is_not_null(&self) -> bool {
        self.not_null
    }

    pub fn is_fixed_length(&self) -> bool {
        self.fixed_length
    }

    /// offset to the datum start in the tuple, for a fixed length data type,
    /// or offset to the datum slot in the tuple, for a var length data type
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// the datum index location in table
    /// the database would reorder the column location by sorting column's data type
    pub fn datum_index(&self) -> u32 {
        self.datum_index
    }

    /// original index in the table schema, eg. in column define list in create table stmt
    pub fn original_index(&self) -> u32 {
        self.original_index
    }

    pub fn datum_max_length(&self) -> u32 {
        self.max_length
    }

    pub fn datum_oper(&self) -> &DatumOper {
        &self.datum_oper
    }
}
