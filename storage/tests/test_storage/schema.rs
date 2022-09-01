use std::fs;

use serde_json::from_str;

use crate::test_storage::test_case_file::test_case_file_path;
use common::error_type::ET;
use common::result::io_result;
use storage::access::schema::TableSchema;

#[allow(dead_code)]
#[cfg(test)]
pub fn load_schema(json_file_name: String) -> Result<TableSchema, ET> {
    let path = test_case_file_path(json_file_name)?;
    let rc = fs::read_to_string(path);
    let context = io_result(rc)?;

    let schema = match from_str::<TableSchema>(context.as_str()) {
        Ok(d) => d,
        Err(e) => {
            println!("deserialize table_schema {}\n", e.to_string());
            return Err(ET::SerdeError(e.to_string()));
        }
    };
    Ok(schema)
}
