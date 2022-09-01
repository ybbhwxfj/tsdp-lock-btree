use std::collections::HashSet;
use std::ops::Range;

use log::trace;
use rand::prelude::SliceRandom;
use rand::rngs::ThreadRng;
use rand::Rng;

use common::error_type::ET;
use storage::access::bt_key_raw::BtKeyRaw;
use storage::access::page_id::PageId;
use storage::access::tuple_desc::TupleDesc;
use storage::access::tuple_items::TupleItems;
use storage::access::tuple_raw::TupleRaw;
use storage::access::tuple_raw_mut::TupleRawMut;
use storage::d_type::data_type::DataType;

use storage::d_type::data_value::ItemValue;
use storage::d_type::integer::IntegerItem;

use crate::test_storage::read_csv::{read_from_csv_file, string_record_to_key_value};

#[cfg(test)]
pub fn generate_key<'a>(
    key_desc: &TupleDesc,
    key_buffer: &'a mut Vec<u8>,
    rnd: &mut ThreadRng,
    set: &mut HashSet<TupleItems>,
) -> Result<BtKeyRaw<'a>, ET> {
    let mut non = HashSet::new();
    gen_key_gut(true, key_desc, key_buffer, rnd, set, &mut non)
}

#[allow(dead_code)]
#[cfg(test)]
pub fn generate_non_exist_key<'a>(
    key_desc: &TupleDesc,
    key_buffer: &'a mut Vec<u8>,
    rnd: &mut ThreadRng,
    set: &mut HashSet<TupleItems>,
    non_set: &mut HashSet<TupleItems>,
) -> Result<BtKeyRaw<'a>, ET> {
    gen_key_gut(false, key_desc, key_buffer, rnd, set, non_set)
}

#[cfg(test)]
fn gen_key_gut<'a>(
    exist: bool,
    key_desc: &TupleDesc,
    key_buffer: &'a mut Vec<u8>,
    rnd: &mut ThreadRng,
    set: &mut HashSet<TupleItems>,
    non_set: &mut HashSet<TupleItems>,
) -> Result<BtKeyRaw<'a>, ET> {
    let len;
    loop {
        let mut vec: Vec<ItemValue> = Vec::new();

        for d in key_desc.datum_desc() {
            if d.column_data_type() == DataType::Integer {
                let i = rnd.gen_range(Range {
                    start: 0,
                    end: i32::MAX,
                });
                vec.push(ItemValue::Integer(IntegerItem::new(i)))
            } else {
                panic!("TODO ...")
            }
        }
        let tuple = TupleItems::from_items(vec);
        if exist {
            if !set.contains(&tuple) {
                set.insert(tuple.clone());
            } else {
                continue;
            }
        } else {
            if !set.contains(&tuple) && !non_set.contains(&tuple) {
                (*non_set).insert(tuple.clone());
            } else {
                continue;
            }
        }
        let tuple = TupleRawMut::from_typed_items(
            key_buffer.as_mut_slice(),
            key_desc.tuple_oper(),
            &tuple.items(),
        )?;

        len = tuple.len();
        break;
    }
    let key = BtKeyRaw::from_slice(&key_buffer.as_slice()[0..len as usize]);
    // trace!("gen key: {}", key.to_json(key_desc).unwrap().to_string());
    return Ok(key);
}

pub struct Generator {
    key_desc: TupleDesc,
    value_desc: TupleDesc,
    rnd: ThreadRng,
    next_page_id: PageId,
    key_buffer: Vec<u8>,
    key_set: HashSet<TupleItems>,
    values: Vec<Vec<u8>>,
}

#[allow(dead_code)]
impl Generator {
    pub fn new(key_desc: TupleDesc, value_desc: TupleDesc) -> Result<Generator, ET> {
        let mut file = read_from_csv_file("data.csv".to_string())?;
        let rnd = ThreadRng::default();
        let mut values: Vec<Vec<u8>> = Vec::new();
        let mut key_buf = vec![];
        let mut value_buf = vec![];
        key_buf.resize(key_desc.max_size() as usize, 0);
        value_buf.resize(value_desc.max_size() as usize, 0);

        for rcd in file.records() {
            let str_rcd = match rcd {
                Ok(r) => r,
                Err(e) => {
                    return Err(ET::CSVError(e.to_string()));
                }
            };

            let (_key, value) = string_record_to_key_value(
                &str_rcd,
                &mut key_buf,
                &mut value_buf,
                &key_desc,
                &value_desc,
            )?;

            values.push(Vec::from(value.slice()))
        }
        Ok(Generator {
            key_desc,
            value_desc,
            rnd,
            next_page_id: 1,
            key_buffer: key_buf,
            key_set: Default::default(),
            values,
        })
    }

    fn gen_key(&mut self) -> Result<Vec<u8>, ET> {
        let key = generate_key(
            &self.key_desc,
            &mut self.key_buffer,
            &mut self.rnd,
            &mut self.key_set,
        )?;
        Ok(Vec::from(key.slice()))
    }

    fn gen_value(&mut self) -> Result<Vec<u8>, ET> {
        let opt_val = self.values.choose(&mut self.rnd);
        let v = match opt_val {
            Some(v) => v,
            None => {
                panic!("error")
            }
        };
        let tuple = TupleRaw::from_slice(v.as_slice());
        trace!(
            "gen value: {}",
            tuple
                .to_json(&self.value_desc.tuple_oper())
                .unwrap()
                .to_string()
        );

        Ok(Vec::from(v.as_slice()))
    }

    pub fn gen_page_id(&mut self) -> Result<PageId, ET> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        Ok(page_id)
    }

    fn gen_two_key_set(
        &mut self,
        left_num: u32,
        right_num: u32,
    ) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), ET> {
        let mut keys1 = vec![];
        assert!(left_num > 2 && right_num > 2);
        for _ in 0..left_num + right_num {
            let key = self.gen_key()?;
            keys1.push(key);
        }

        keys1.sort_by(|x, y| {
            let xk = BtKeyRaw::from_slice(x.as_slice());
            let yk = BtKeyRaw::from_slice(y.as_slice());
            BtKeyRaw::compare(&xk, &yk, self.key_desc.tuple_oper())
        });

        let max = match keys1.pop() {
            Some(v) => v,
            None => {
                panic!("error")
            }
        };

        let mut rnd = ThreadRng::default();
        keys1.shuffle(&mut rnd);
        let mut keys2 = keys1.split_off(left_num as usize);
        keys2.push(max);
        Ok((keys1, keys2))
    }

    pub fn gen_key_value(&mut self) -> Result<(Vec<u8>, Vec<u8>), ET> {
        let k = self.gen_key()?;
        let v = self.gen_value()?;
        Ok((k, v))
    }

    pub fn gen_key_page_id(&mut self) -> Result<(Vec<u8>, PageId), ET> {
        let k = self.gen_key()?;
        let v = self.gen_page_id()?;
        Ok((k, v))
    }

    // Return Value :
    //      (Vx<v1, v2, v3>, Vy<v4, v5>)
    // Vx: key values in page
    //      v1: key
    //      v2: value
    //      v3: value
    // Vy: key values to insert/update
    //      v4: key
    //      v5: value
    pub fn split_leaf_data(
        &mut self,
        left_num: u32,
        right_num: u32,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>), ET> {
        let (keys1, keys2) = self.gen_two_key_set(left_num, right_num)?;

        let mut vec1 = vec![];
        let mut vec2 = vec![];
        for k in keys1 {
            let v1 = self.gen_value()?;
            let v2 = self.gen_value()?;
            vec1.push((k, v1, v2));
        }
        for k in keys2 {
            let v = self.gen_value()?;
            vec2.push((k, v));
        }

        assert!(vec1.len() > 1);
        assert!(vec2.len() > 1);
        Ok((vec1, vec2))
    }

    // Return Value :
    //      (Vx<v1, v2, v3>, Vy<v4, v5, v6>)
    // Vx: key id in page
    //      v1: key
    //      v2: key
    //      v3: id
    // Vy: key id to insert/update
    //      v4: key
    //      v5: key
    //      v6: id
    pub fn split_non_leaf_data(
        &mut self,
        is_le: bool,
        left_num: u32,
        right_num: u32,
    ) -> Result<
        (
            Vec<(Vec<u8>, Vec<u8>, PageId)>,
            Vec<(Vec<u8>, Vec<u8>, PageId)>,
        ),
        ET,
    > {
        let mut keys1 = vec![];
        assert!(left_num > 2 && right_num > 2);
        for _ in 0..(left_num + right_num) * 2 {
            let key = self.gen_key()?;
            keys1.push(key);
        }

        keys1.sort_by(|x, y| {
            let xk = BtKeyRaw::from_slice(x.as_slice());
            let yk = BtKeyRaw::from_slice(y.as_slice());
            BtKeyRaw::compare(&xk, &yk, self.key_desc.tuple_oper())
        });
        let max1 = match keys1.pop() {
            Some(v) => v,
            None => {
                panic!("error")
            }
        };
        let max2 = match keys1.pop() {
            Some(v) => v,
            None => {
                panic!("error")
            }
        };
        let len = keys1.len();
        let mut vec3 = vec![];
        for i in 0..len {
            if i + 1 < len && i % 2 == 0 {
                let (v1, v2) = unsafe {
                    let v1 = keys1.get_unchecked(i);
                    let v2 = keys1.get_unchecked(i + 1);
                    (v1.clone(), v2.clone())
                };
                let page_id = self.gen_page_id()?;
                let k_id = if is_le {
                    let x = BtKeyRaw::from_slice(v1.as_slice());
                    let y = BtKeyRaw::from_slice(v2.as_slice());
                    trace!(
                        "<: {} < {}",
                        x.to_json(self.key_desc.tuple_oper()).unwrap().to_string(),
                        y.to_json(self.key_desc.tuple_oper()).unwrap().to_string()
                    );
                    (v1, v2, page_id)
                } else {
                    let x = BtKeyRaw::from_slice(v1.as_slice());
                    let y = BtKeyRaw::from_slice(v2.as_slice());
                    trace!(
                        ">: {} > {}",
                        x.to_json(self.key_desc.tuple_oper()).unwrap().to_string(),
                        y.to_json(self.key_desc.tuple_oper()).unwrap().to_string()
                    );
                    (v2, v1, page_id)
                };
                vec3.push(k_id);
            }
        }
        let mut rnd = ThreadRng::default();
        vec3.shuffle(&mut rnd);
        let mut vec4 = vec3.split_off(left_num as usize);
        let id1 = self.gen_page_id()?;
        let id2 = self.gen_page_id()?;
        vec4.push((max1.clone(), max2.clone(), id1));
        vec4.push((max2.clone(), max1.clone(), id2));
        Ok((vec3, vec4))
    }
}
