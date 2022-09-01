use std::collections::HashMap;

use adt::compare::Compare;
use common::error_type::ET;
use common::id::OID;
use common::result::Result;

use crate::access::bt_key_raw::BtKeyRawCmp;
use crate::access::datum_desc::DatumDesc;
use crate::access::schema::TableSchema;
use crate::access::to_json::ToJson;
use crate::access::tuple_desc::TupleDesc;
use crate::access::tuple_to_json::TupleToJson;
use crate::d_type::data_type::DataType;
use crate::d_type::data_type_func::DataTypeInit;

pub struct TableContext<C: Compare<[u8]> + 'static, KT: ToJson + 'static, VT: ToJson + 'static> {
    pub key_desc: TupleDesc,
    pub value_desc: TupleDesc,
    pub name2desc: HashMap<String, DatumDesc>,
    pub id2desc: HashMap<OID, DatumDesc>,
    pub compare: C,
    pub key2json: KT,
    pub value2json: VT,
}

impl<C: Compare<[u8]> + 'static, KT: ToJson + 'static, VT: ToJson + 'static>
    TableContext<C, KT, VT>
{
    pub fn get_desc_by_name(&self, column_name: &String) -> Result<DatumDesc> {
        let opt = self.name2desc.get(column_name);
        match opt {
            Some(d) => Ok(d.clone()),
            None => Err(ET::NoSuchElement),
        }
    }

    pub fn get_desc_by_id(&self, id: &OID) -> Result<DatumDesc> {
        let opt = self.id2desc.get(id);
        match opt {
            Some(d) => Ok(d.clone()),
            None => Err(ET::NoSuchElement),
        }
    }
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson + 'static, VT: ToJson + 'static> Send
    for TableContext<C, KT, VT>
{
}

unsafe impl<C: Compare<[u8]> + 'static, KT: ToJson + 'static, VT: ToJson + 'static> Sync
    for TableContext<C, KT, VT>
{
}

pub fn schema_to_context(
    schema: &TableSchema,
    map: &HashMap<DataType, DataTypeInit>,
) -> Result<TableContext<BtKeyRawCmp, TupleToJson, TupleToJson>> {
    let mut id2desc: HashMap<OID, DatumDesc> = HashMap::new();
    let mut name2desc: HashMap<String, DatumDesc> = HashMap::new();
    let (key_desc, value_desc) = TupleDesc::from_table_schema(schema, map)?;
    for vec in [&key_desc.datum_desc, &value_desc.datum_desc] {
        for desc in vec {
            let opt = id2desc.insert(desc.column_id(), desc.clone());
            if opt.is_some() {
                return Err(ET::ExistingSuchElement);
            }
        }
    }
    for cs in &schema.columns {
        let opt_desc = id2desc.get(&cs.column_id);
        match opt_desc {
            Some(desc) => {
                let opt = name2desc.insert(cs.name.clone(), desc.clone());
                if opt.is_some() {
                    return Err(ET::ExistingSuchElement);
                }
            }
            None => {
                return Err(ET::NoSuchElement);
            }
        }
    }
    let compare = BtKeyRawCmp::new(key_desc.tuple_oper().clone());
    let key2json = TupleToJson::new(key_desc.tuple_oper().clone());
    let value2json = TupleToJson::new(value_desc.tuple_oper().clone());
    let ctx = TableContext {
        key_desc,
        value_desc,
        name2desc,
        id2desc,
        compare,
        key2json,
        value2json,
    };
    Ok(ctx)
}
