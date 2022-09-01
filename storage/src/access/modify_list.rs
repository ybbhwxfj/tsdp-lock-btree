use crate::access::page_id::PageId;
use crate::access::to_json::ToJson;
use common::result::Result;
use json::{object, JsonValue};
use std::collections::HashMap;

/// when a high key of a child page was updated, we must update the parent page who pointer this
/// child page.
/// We define 3 types of operations, insert/update/delete on non leaf node
/// ModifyList keeps all the operations when update a high key
///
#[derive(Clone, Copy, Debug)]
pub enum ParentModifyOp {
    ParentInsertSub,
    ParentUpdateSub,
    ParentDeleteSub,
}

#[derive(Clone, Debug)]
pub struct ParentModify {
    pub child_id: PageId,
    pub sequence: u64,
    pub modify_op: ParentModifyOp,
    pub old_high_key: Option<Vec<u8>>,
    pub new_high_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ModifyList {
    id2modify: HashMap<PageId, ParentModify>,
    history: Vec<ParentModify>,
    num_new_page: u64,
    sequence: u64,
    trace_info: HashMap<u64, String>,
}

impl ModifyList {
    pub fn new() -> Self {
        Self {
            id2modify: Default::default(),
            history: vec![],
            num_new_page: 0,
            sequence: 0,
            trace_info: Default::default(),
        }
    }

    pub fn num_new_page(&self) -> u64 {
        self.num_new_page
    }

    pub fn new_page(&mut self, page_id: PageId) {
        if !self.id2modify.contains_key(&page_id) {
            self.num_new_page += 1;
        }
    }

    pub fn append_trace(&mut self, msg: String) {
        self.sequence += 1;
        let _ = self.trace_info.insert(self.sequence, msg);
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn add_operation(
        &mut self,
        page_id: PageId,
        op: ParentModifyOp,
        old: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) {
        self.sequence += 1;

        let m = ParentModify {
            child_id: page_id,
            sequence: self.sequence,
            modify_op: op.clone(),
            old_high_key: old.clone(),
            new_high_key: new.clone(),
        };
        self.history.push(m);
        let opt = self.id2modify.get_mut(&page_id);
        match opt {
            Some(m) => {
                let op1 = match op {
                    ParentModifyOp::ParentInsertSub => {
                        if m.old_high_key.is_some() {
                            assert!(new.is_some());
                            ParentModifyOp::ParentUpdateSub
                        } else {
                            op
                        }
                    }
                    ParentModifyOp::ParentUpdateSub => {
                        if m.old_high_key.is_some() {
                            assert!(new.is_some());
                            op
                        } else {
                            ParentModifyOp::ParentInsertSub
                        }
                    }
                    _ => op,
                };
                m.modify_op = op1;
                m.new_high_key = new;
            }
            None => {
                let modify = ParentModify {
                    child_id: page_id,
                    sequence: self.sequence,
                    modify_op: op,
                    old_high_key: old,
                    new_high_key: new,
                };
                self.id2modify.insert(page_id, modify);
            }
        }
    }

    /// test contain a page id operation,
    /// if it exists, ParentModification Lock must be acquired before search its parent
    pub fn contain(&self, page_id: PageId) -> bool {
        self.id2modify.contains_key(&page_id)
    }

    pub fn move_to_list(self) -> Vec<ParentModify> {
        let mut vec = Vec::new();
        for (_id, m) in self.id2modify {
            vec.push(m);
        }
        // This sort is stable
        vec.sort_by(|x, y| x.sequence.cmp(&y.sequence).reverse());
        vec
    }

    pub fn modify_page_ids(&self) -> Vec<PageId> {
        let mut vec = Vec::new();
        for (_id, m) in &self.id2modify {
            vec.push(m.child_id)
        }
        vec
    }

    pub fn is_empty(&self) -> bool {
        self.id2modify.is_empty()
    }

    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.id2modify, &mut other.id2modify);
        std::mem::swap(&mut self.num_new_page, &mut other.num_new_page);
        std::mem::swap(&mut self.sequence, &mut other.sequence);
        std::mem::swap(&mut self.history, &mut other.history);
    }

    pub fn to_json<K2J: ToJson>(&self, key2json: &K2J) -> Result<JsonValue> {
        let mut vec = Vec::new();
        let mut history = Vec::new();
        for (_, m) in &self.id2modify {
            let j = Self::op_to_json(m, key2json)?;
            vec.push(j);
        }
        for m in &self.history {
            let j = Self::op_to_json(m, key2json)?;
            history.push(j);
        }
        Ok(object! {"list": vec,
            "history":history,
            "trace" : format!("{:?}", self.trace_info)
        })
    }

    pub fn op_to_json<K2J: ToJson>(op: &ParentModify, key2json: &K2J) -> Result<JsonValue> {
        let value = match op.modify_op {
            ParentModifyOp::ParentInsertSub => {
                assert!(op.new_high_key.is_some());
                object! {
                    "op" : "insert",
                    "high_key": Self::opt_vec_to_json(&op.new_high_key, key2json)?,
                }
            }
            ParentModifyOp::ParentUpdateSub => {
                assert!(op.new_high_key.is_some());
                assert!(op.old_high_key.is_some());
                object! {
                    "op" : "update",
                    "old_high_key": Self::opt_vec_to_json(&op.old_high_key, key2json)?,
                    "new_high_key": Self::opt_vec_to_json(&op.new_high_key, key2json)?,
                }
            }
            ParentModifyOp::ParentDeleteSub => {
                assert!(op.old_high_key.is_some());
                object! {
                "op" : "delete",
                    "high_key": Self::opt_vec_to_json(&op.old_high_key, key2json)?,
                }
            }
        };
        let j = object! {
            "page_id" : op.child_id,
            "sequence" : op.sequence,
            "operation" : value,
        };
        Ok(j)
    }
    fn opt_vec_to_json<K2J: ToJson>(
        opt_key: &Option<Vec<u8>>,
        key2json: &K2J,
    ) -> Result<JsonValue> {
        let jv = match opt_key {
            Some(k) => key2json.to_json(k)?,
            None => {
                object! {}
            }
        };
        Ok(jv)
    }
}
