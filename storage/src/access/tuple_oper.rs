use crate::access::datum_oper::DatumOper;

#[derive(Clone)]
pub struct TupleOper {
    slot_offset_begin: u32,
    slot_offset_end: u32,
    vec: Vec<DatumOper>,
}

impl Default for TupleOper {
    fn default() -> Self {
        Self {
            slot_offset_begin: 0,
            slot_offset_end: 0,
            vec: vec![],
        }
    }
}

impl TupleOper {
    pub fn new(slot_begin: u32, slot_end: u32, vec: Vec<DatumOper>) -> Self {
        TupleOper {
            slot_offset_begin: slot_begin,
            slot_offset_end: slot_end,
            vec,
        }
    }

    pub fn add_datum_oper(&mut self, oper: DatumOper) {
        self.vec.push(oper);
    }

    pub fn datum_oper(&self) -> &Vec<DatumOper> {
        &self.vec
    }

    pub fn slot_offset_begin(&self) -> u32 {
        self.slot_offset_begin
    }

    pub fn slot_offset_end(&self) -> u32 {
        self.slot_offset_end
    }
}
