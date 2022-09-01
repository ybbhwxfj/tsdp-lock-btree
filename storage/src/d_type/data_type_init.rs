use std::cell::RefCell;

use crate::d_type;
use crate::d_type::data_type::DataType;
use crate::d_type::data_type_func;
use crate::d_type::data_value::ItemValue;
use crate::d_type::datum_item::DatumItem;
use crate::d_type::double::DoubleItem;
use crate::d_type::fixchar::CharItem;
use crate::d_type::float::FloatItem;
use crate::d_type::integer::IntegerItem;
use crate::d_type::long::LongItem;

fn fn_datum_item_default<T: DatumItem + Default + 'static>() -> Box<RefCell<dyn DatumItem>> {
    let cell = Box::new(RefCell::new(T::default()));
    cell
}

pub fn mapping_data_type() -> Vec<data_type_func::DataTypeInit> {
    let vec = vec![
        data_type_func::DataTypeInit::new(
            DataType::Integer,
            ItemValue::Integer(Default::default()),
            d_type::integer::fn_cmp_i32,
            fn_datum_item_default::<IntegerItem>,
        ),
        data_type_func::DataTypeInit::new(
            DataType::Long,
            ItemValue::Long(Default::default()),
            d_type::long::fn_cmp_i64,
            fn_datum_item_default::<LongItem>,
        ),
        data_type_func::DataTypeInit::new(
            DataType::Float,
            ItemValue::Float(Default::default()),
            d_type::float::fn_cmp_f32,
            fn_datum_item_default::<FloatItem>,
        ),
        data_type_func::DataTypeInit::new(
            DataType::Double,
            ItemValue::Double(Default::default()),
            d_type::double::fn_cmp_f64,
            fn_datum_item_default::<DoubleItem>,
        ),
        data_type_func::DataTypeInit::new(
            DataType::VarChar,
            ItemValue::VarChar(Default::default()),
            d_type::varchar::fn_cmp_string,
            fn_datum_item_default::<CharItem>,
        ),
        data_type_func::DataTypeInit::new(
            DataType::Char,
            ItemValue::Char(Default::default()),
            d_type::fixchar::fn_cmp_char,
            fn_datum_item_default::<CharItem>,
        ),
    ];
    vec
}
