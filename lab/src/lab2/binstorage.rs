use crate::lab2::client::StorageClient;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, Storage};

use super::virtualized_bin_client::VirBinStorageClient;

pub struct BinStorageClient {
    pub list_back: Vec<String>,
}

// the following function is from Rust's official example
fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let hash = calculate_hash(&name.clone()) as usize;
        let length = self.list_back.len();
        let index = hash % length;
        let addr = self.list_back[index].clone();

        // parameter `addr` is in the form of <host>:<port>, and it is always a valid TCP address
        // returned Storage is used as an interface

        let sc = <VirBinStorageClient as super::virtualized_bin_client::Newhack>::neww(
            &self.list_back,
            name,
        )
        .await;

        return Ok(Box::new(sc));
    }
}

// pub struct testAddr {
//     list_back: Vec<String>,
// }

// impl testAddr {
//     fn cal_addr(&self, name: String) -> String {
//         let hash = calculate_hash(&name.clone()) as usize;
//         let length = self.list_back.len();
//         let index = hash % length;
//         let addr = self.list_back[index].clone();
//         return addr;
//     }
// }

// fn main() -> () {
//     let addrs = vec!["101.1".to_string(), "102.2".to_string(), "103.3".to_string(), "104.4".to_string() , "105.5".to_string(), "106.6".to_string(),];

//     let people = vec!["Alice1", "Bob1","Alice2", "Bob2","Alice3", "Bob3","Alice1", "Bob1",];

//     let tm = testAddr {
//         list_back: addrs,
//     };

//     for person in people {
//         let addr = tm.cal_addr(person.to_string());
//         print!("Name {} is mapped to {}", person, addr);
//     }

// }
