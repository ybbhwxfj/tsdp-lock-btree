use json::JsonValue;

use common::result::Result;

pub trait ToJson: Clone {
    fn to_json(&self, value: &[u8]) -> Result<JsonValue>;
}

pub trait SelfToJson: Clone {
    fn to_json(&self) -> Result<JsonValue>;
}

pub struct EmptyToJson {}

impl Clone for EmptyToJson {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl ToJson for EmptyToJson {
    fn to_json(&self, _key: &[u8]) -> Result<JsonValue> {
        Ok(JsonValue::Null)
    }
}

unsafe impl Sync for EmptyToJson {}

unsafe impl Send for EmptyToJson {}
