use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn cons_hash(val: &String) -> u64 {
    // todo: need to distinguish between ip addr, log and bin names
    calculate_hash(val)
}
pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
