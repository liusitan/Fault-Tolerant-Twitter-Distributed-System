use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn bin_aware_cons_hash(val: &String) -> u64 {
    // "bin#Alice|"
    if val.starts_with("bin") {
        // extract the bin name
        let tokens: Vec<&str> = val.split("|").collect();
        let sub_tokens: Vec<&str> = tokens[0].split("#").collect();
        let binname = sub_tokens[1].to_string();
        return cons_hash(&binname);
    } else {
        return cons_hash(val);
    }
}

pub fn cons_hash(val: &String) -> u64 {
    // todo: need to distinguish between ip addr, log and bin names
    calculate_hash(val)
}
pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
