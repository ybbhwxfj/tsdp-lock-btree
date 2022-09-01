use std::collections::HashMap;
use std::hash::Hash;

use crate::compare::IsEqual;
use crate::hash_fn::HashFn;

struct HashKey<K: Clone, H: HashFn<K>, E: IsEqual<K>> {
    key: K,
    hasher: H,
    equal: E,
}

impl<Key: Clone, H: HashFn<Key>, E: IsEqual<Key>> HashKey<Key, H, E> {
    pub fn new(key: Key, hasher: H, equal: E) -> Self {
        Self { key, hasher, equal }
    }
}

impl<K: Clone, H: HashFn<K>, E: IsEqual<K>> Hash for HashKey<K, H, E> {
    fn hash<HA: std::hash::Hasher>(&self, state: &mut HA) {
        let hash = self.hasher.hash(&self.key);
        hash.hash(state)
    }
}

impl<Key: Clone, H: HashFn<Key>, E: IsEqual<Key>> PartialEq<Self> for HashKey<Key, H, E> {
    fn eq(&self, other: &Self) -> bool {
        self.equal.equal(&self.key, &other.key)
    }
}

impl<K: Clone, H: HashFn<K>, E: IsEqual<K>> Eq for HashKey<K, H, E> {}

pub struct UnorderedMap<K: Clone, V, H: HashFn<K>, E: IsEqual<K>> {
    hasher: H,
    equal: E,
    hash_map: HashMap<HashKey<K, H, E>, V>,
}

impl<K: Clone, V, H: HashFn<K>, E: IsEqual<K>> UnorderedMap<K, V, H, E> {
    pub fn new(hasher: H, equal: E) -> Self {
        Self {
            hasher,
            equal,
            hash_map: std::collections::HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash_key = HashKey::new(key, self.hasher.clone(), self.equal.clone());
        self.hash_map.insert(hash_key, value)
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let hash_key = HashKey::new(key.clone(), self.hasher.clone(), self.equal.clone());
        self.hash_map.get(&hash_key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let hash_key = HashKey::new(key.clone(), self.hasher.clone(), self.equal.clone());
        self.hash_map.get_mut(&hash_key)
    }
}
