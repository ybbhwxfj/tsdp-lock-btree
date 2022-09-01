use std::fs::File;

use csv::{StringRecord, Trim};

use crate::test_storage::test_case_file::test_case_file_path;
use common::error_type::ET;
use common::result::io_result;
use storage::access::bt_key_raw::BtKeyRaw;
use storage::access::const_val;
use storage::access::tuple_desc::TupleDesc;
use storage::access::tuple_raw::TupleRaw;
use storage::access::tuple_raw_mut::TupleRawMut;

#[allow(dead_code)]
#[cfg(test)]
pub fn create_csv_file(path: String) -> Result<csv::Writer<File>, ET> {
    let file = io_result(File::create(path))?;
    let writer = csv::Writer::from_writer(file);
    Ok(writer)
}

#[allow(dead_code)]
pub fn write_csv_row(
    key: &[u8],
    value: &[u8],
    key_desc: &TupleDesc,
    value_desc: &TupleDesc,
    writer: &mut csv::Writer<File>,
) -> Result<(), ET> {
    let mut index_item = vec![];
    for (_i, (s, desc)) in [(key, key_desc), (value, value_desc)].iter().enumerate() {
        let tuple = TupleRaw::from_slice(s);
        for (i, d) in desc.tuple_oper().datum_oper().iter().enumerate() {
            let slice = d.tuple_get_datum(tuple.slice());
            let str = d.datum_to_string(slice)?;
            let index = desc.datum_desc()[i].original_index();
            index_item.push((index, str));
        }
    }
    index_item.sort_by(|(x, _), (y, _)| x.cmp(y));
    let mut vec = vec![];
    for (_, s) in index_item {
        vec.push(s);
    }
    let record = csv::StringRecord::from(vec);
    let r = writer.write_record(&record);
    match r {
        Err(e) => Err(ET::CSVError(e.to_string())),
        Ok(()) => Ok(()),
    }
}

#[cfg(test)]
pub fn read_from_csv_file(csv_file_name: String) -> Result<csv::Reader<File>, ET> {
    let path = test_case_file_path(csv_file_name)?;

    let rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .double_quote(false)
        .escape(Some(b'\\'))
        .flexible(true)
        .comment(Some(b'#'))
        .trim(Trim::All)
        .from_path(path);

    match rdr {
        Ok(r) => Ok(r),
        Err(e) => Err(ET::CSVError(e.to_string())),
    }
}

pub fn string_record_to_key_value<'a, 'b>(
    str_rcd: &StringRecord,
    key_buffer: &'a mut Vec<u8>,
    value_buffer: &'b mut Vec<u8>,
    key_desc: &TupleDesc,
    value_desc: &TupleDesc,
) -> Result<(BtKeyRaw<'a>, TupleRaw<'b>), ET> {
    let mut key_size = const_val::TUPLE_HEADER_SIZE;
    let mut value_size = const_val::TUPLE_HEADER_SIZE;
    for (i, tuple_desc) in [key_desc, value_desc].iter().enumerate() {
        let mut var_offset = tuple_desc.slot_offset_end();
        for (n, datum_desc) in tuple_desc.tuple_oper().datum_oper().iter().enumerate() {
            let fixed_length = datum_desc.is_fixed_length();
            let index = tuple_desc.datum_desc()[n].original_index();
            let s = match str_rcd.get(index as usize) {
                Some(s) => s,
                None => {
                    return Err(ET::NoneOption);
                }
            };
            let str = s.to_string();
            let mut tuple = if i == 0 {
                TupleRawMut::from_slice(key_buffer.as_mut_slice())
            } else {
                TupleRawMut::from_slice(value_buffer.as_mut_slice())
            };
            let r = tuple.from_string(&str, datum_desc, var_offset);
            match r {
                Ok(size) => {
                    if !fixed_length {
                        // variable length datum add size
                        var_offset += size;
                    } else {
                    }
                }
                Err(_e) => {
                    panic!("todo")
                }
            }
        }
        if i == 0 {
            key_size = var_offset;
        } else {
            value_size = var_offset;
        }
    }
    assert!(key_buffer.len() >= key_size as usize);
    assert!(value_buffer.len() >= value_size as usize);

    let mut t1 = TupleRawMut::from_slice(key_buffer.as_mut_slice());
    t1.set_size(key_size);
    let mut t2 = TupleRawMut::from_slice(value_buffer.as_mut_slice());
    t2.set_size(value_size);
    let key = BtKeyRaw::from_slice(&key_buffer.as_slice()[0..key_size as usize]);
    let value = TupleRaw::from_slice(&value_buffer.as_slice()[0..value_size as usize]);

    // trace!("{}", key.to_json(&key_desc).unwrap().to_string());
    // trace!("{}", value.to_json(&value_desc).unwrap().to_string());

    return Ok((key, value));
}
