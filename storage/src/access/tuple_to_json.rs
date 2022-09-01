use json::JsonValue;

use common::error_type::ET;

use crate::access::bt_key_raw::BtKeyRaw;
use crate::access::to_json::ToJson;

use crate::access::tuple_oper::TupleOper;

#[derive(Clone)]
pub struct TupleToJson {
    key_desc: TupleOper,
}

impl TupleToJson {
    pub fn new(key_desc: TupleOper) -> TupleToJson {
        TupleToJson { key_desc }
    }
}

impl ToJson for TupleToJson {
    fn to_json(&self, key: &[u8]) -> Result<JsonValue, ET> {
        let k = BtKeyRaw::from_slice(key);
        k.to_json(&self.key_desc)
    }
}
