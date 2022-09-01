use std::collections::HashMap;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use common::id::OID;

use crate::access::cursor::Cursor;
use crate::access::page_id::PageId;

#[derive(Clone, Serialize, Deserialize)]
pub struct SearchOption {}

#[derive(Clone, Serialize, Deserialize)]
pub struct DeleteOption {
    /// remove the key directly from index
    /// use mark delete if false
    pub remove_key: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InsertOption {
    pub update_if_exist: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UpdateOption {
    pub insert_if_not_exist: bool,
}

pub struct ConflictSet {
    pub predicate: Vec<OID>,
    pub key: Vec<Vec<u8>>,
}

pub struct InstallSet {
    pub key: HashMap<Vec<u8>, PageId>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TxOption {
    conflict_predicate: Vec<OID>,
    conflict_key: Vec<Vec<u8>>,
    checked_key: HashSet<Vec<u8>>,
    checked_predicate: HashSet<OID>,
    key2page: HashMap<Vec<u8>, PageId>,
    cursor: Cursor,
}

impl Default for InsertOption {
    fn default() -> Self {
        InsertOption {
            update_if_exist: false,
        }
    }
}

impl Default for UpdateOption {
    fn default() -> Self {
        UpdateOption {
            insert_if_not_exist: false,
        }
    }
}

impl DeleteOption {
    pub fn new() -> Self {
        Self { remove_key: false }
    }
}

impl Default for SearchOption {
    fn default() -> Self {
        SearchOption {}
    }
}

impl Default for DeleteOption {
    fn default() -> Self {
        Self { remove_key: true }
    }
}

impl Default for ConflictSet {
    fn default() -> Self {
        Self {
            predicate: vec![],
            key: vec![],
        }
    }
}

impl Default for InstallSet {
    fn default() -> Self {
        Self {
            key: HashMap::new(),
        }
    }
}

impl Default for TxOption {
    fn default() -> Self {
        Self::new()
    }
}

impl TxOption {
    pub fn new() -> Self {
        Self {
            conflict_predicate: vec![],
            conflict_key: vec![],

            checked_key: Default::default(),
            checked_predicate: Default::default(),
            key2page: Default::default(),
            cursor: Default::default(),
        }
    }

    pub fn add_conflict_predicate(&mut self, oid: OID) -> bool {
        return if !self.checked_predicate.contains(&oid) {
            self.conflict_predicate.push(oid);
            true
        } else {
            false
        };
    }

    pub fn add_conflict_key(&mut self, key: Vec<u8>) -> bool {
        if !self.checked_key.contains(&key) {
            self.conflict_key.push(key);
            true
        } else {
            false
        }
    }

    pub fn add_set_key(&mut self, key: Vec<u8>, page_id: PageId) {
        let _ = self.key2page.insert(key, page_id);
    }

    pub fn mutable_cursor(&mut self) -> &mut Cursor {
        &mut self.cursor
    }

    pub fn opt_cursor(&self) -> Option<Cursor> {
        if self.cursor.is_invalid() {
            None
        } else {
            Some(self.cursor.clone())
        }
    }

    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub fn get_conflict_key(&mut self) -> &Vec<Vec<u8>> {
        &self.conflict_key
    }

    pub fn get_conflict_predicate(&mut self) -> &Vec<OID> {
        &self.conflict_predicate
    }

    pub fn install_set(&mut self) -> InstallSet {
        let mut r = InstallSet::default();
        std::mem::swap(&mut self.key2page, &mut r.key);

        return r;
    }

    pub fn add_conflict_to_checked(&mut self) {
        for key in &self.conflict_key {
            let _ = self.checked_key.insert(key.clone());
        }
        for oid in &self.conflict_predicate {
            let _ = self.checked_predicate.insert(oid.clone());
        }
        self.checked_key.clear();
        self.conflict_predicate.clear();
    }
}
