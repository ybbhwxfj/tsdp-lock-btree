use common::error_type::ET;
use common::result::Result;

use crate::access::tuple_oper::TupleOper;

use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;

pub struct UpdateTuple {
    update_item: Vec<(u32, ItemValue)>,
}

impl UpdateTuple {
    pub fn new(update_item: Vec<(u32, ItemValue)>) -> Self {
        Self { update_item }
    }

    pub fn update(&self, dst_tuple: &mut [u8], dst_oper: &TupleOper) -> Result<Option<Vec<u8>>> {
        let mut insufficient_space = false;
        for (i, item) in &self.update_item {
            let index = *i as usize;
            if index >= dst_oper.datum_oper().len() {
                panic!("error")
            }
            let r = dst_oper.datum_oper()[index].tuple_set_datum(dst_tuple, item);
            match r {
                Ok(_r) => {}
                Err(e) => {
                    if e == ET::ExceedCapacity {
                        insufficient_space = true;
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        if insufficient_space {
            let mut vec = Vec::new();
            let mut items = Vec::new();
            let mut add_n = 0usize;
            for desc in dst_oper.datum_oper() {
                let item = desc.tuple_to_item(dst_tuple)?;
                items.push(item);
            }
            for (_i, item) in &self.update_item {
                add_n += item.size() as usize;
                //items[i as usize] = (*item).clone();
            }
            vec.resize(dst_tuple.len() + add_n, 0);
            let tuple_len = { 0 };

            vec.resize(tuple_len, 0);
            if tuple_len < dst_tuple.len() {
                dst_tuple[0..tuple_len].copy_from_slice(vec.as_slice());
                Ok(None)
            } else {
                Ok(Some(vec))
            }
        } else {
            Ok(None)
        }
    }
}
