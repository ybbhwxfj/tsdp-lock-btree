use storage::access::schema::{ColumnSchema, TableSchema};
use storage::d_type::data_type::DataType;

pub mod test_storage;

#[test]
fn test_create_schema() {
    let mut ts = TableSchema::new("table".to_string());
    let s1 = ColumnSchema::new("c_1".to_string(), DataType::VarChar, true, true, true, 64);
    let s2 = ColumnSchema::new(
        "c_2".to_string(),
        DataType::VarChar,
        false,
        true,
        false,
        128,
    );

    let s3 = ColumnSchema::new(
        "c_3".to_string(),
        DataType::Integer,
        false,
        true,
        false,
        128,
    );

    ts.add_column(s1);
    ts.add_column(s2);
    ts.add_column(s3);
    let r = serde_json::to_string(&ts);
    match r {
        Ok(s) => {
            println!("{}", s);
        }
        Err(e) => {
            panic!("{}", e.to_string())
        }
    }
}
