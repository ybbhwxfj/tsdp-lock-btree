use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use common::result::Result;

use crate::compare::Compare;

struct BTreeKey<K: Clone, C: Compare<K>> {
    compare: C,
    key: K,
}

pub struct OrderedMap<K: Clone, V, C: Compare<K>> {
    compare: C,
    btree: BTreeMap<BTreeKey<K, C>, V>,
}

impl<K: Clone, C: Compare<K>> BTreeKey<K, C> {
    pub fn new(key: K, compare: C) -> Self {
        Self { compare, key }
    }
}

impl<K: Clone, C: Compare<K>> PartialEq<Self> for BTreeKey<K, C> {
    fn eq(&self, other: &Self) -> bool {
        self.compare.compare(&self.key, &other.key).is_eq()
    }
}

impl<K: Clone, C: Compare<K>> Eq for BTreeKey<K, C> {}

impl<K: Clone, C: Compare<K>> PartialOrd<Self> for BTreeKey<K, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare.compare(&self.key, &other.key))
    }
}

impl<K: Clone, C: Compare<K>> Ord for BTreeKey<K, C> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare.compare(&self.key, &other.key)
    }
}

impl<K: Clone, V, C: Compare<K>> OrderedMap<K, V, C> {
    pub fn new(compare: C) -> Self {
        Self {
            compare,
            btree: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let key = BTreeKey::new(k.clone(), self.compare.clone());
        self.btree.insert(key, v)
    }

    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let key = BTreeKey::new(k.clone(), self.compare.clone());

        self.btree.get_mut(&key)
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        let key = BTreeKey::new(k.clone(), self.compare.clone());
        self.btree.get(&key)
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        let key = BTreeKey::new(k.clone(), self.compare.clone());
        self.btree.remove(&key)
    }

    pub fn range<R, F>(&self, range: R, search_kv: F) -> Result<()>
    where
        R: RangeBounds<K>,
        F: Fn(&K, &V) -> Result<()>,
    {
        let low = range
            .start_bound()
            .map(|x| BTreeKey::new(x.clone(), self.compare.clone()));

        let up = range
            .end_bound()
            .map(|x| BTreeKey::new(x.clone(), self.compare.clone()));

        for (k, v) in self.btree.range((low, up)) {
            search_kv(&k.key, v)?;
        }
        Ok(())
    }
}
