use std::collections::HashSet;

use crate::lab1::client;
// use crate::lab1::client::Newhack;
use async_trait::async_trait;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::rpc::Key;
use tribbler::storage;
use tribbler::storage::KeyValue;
use tribbler::storage::List;
use tribbler::storage::Pattern;
use tribbler::storage::{KeyList, KeyString, Storage};
pub struct VirBinStorageClient {
    pub addr: String,
    pub user_name: String,
    pub client1: client::StorageClient,
    pub client2: client::StorageClient,
}

const addr1: &str = "A1";
const addr2: &str = "A2";

const SEPARATOR: &str = "|";
#[async_trait]
pub trait Newhack {
    async fn neww(addr: &str, name: &str) -> VirBinStorageClient;
}
#[async_trait]

impl Newhack for VirBinStorageClient {
    async fn neww(addr: &str, name: &str) -> VirBinStorageClient {
        return VirBinStorageClient {
            addr: addr.to_owned(),
            user_name: name.to_owned(),
            client1: <client::StorageClient as client::Newhack>::neww(addr1).await,
            client2: <client::StorageClient as client::Newhack>::neww(addr2).await,
        };
    }
}
impl VirBinStorageClient {
    fn wrap_with_user_name(&self, s: &str) -> String {
        //$$println!("wrapped name {:?}", self.user_name.clone() + "|" + s);
        self.user_name.clone() + SEPARATOR + s
    }
    fn dewrap_with_user_name(&self, s: &str) -> String {
        let idx = s.find(SEPARATOR).unwrap();
        return s[(idx + 1)..].to_string();
    }
}

#[async_trait] // VERY IMPORTANT !!
impl Storage for VirBinStorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut c1 = match self.client1.clock(at_least).await {
            Ok(x) => x,
            Err(_) => todo!(),
        };
        let mut c2 = match self.client2.clock(at_least).await {
            Ok(x) => x,
            Err(_) => todo!(),
        };
        let mut cmax = std::cmp::max(c1, c2);
        c1 = match self.client1.clock(at_least).await {
            Ok(x) => x,
            Err(_) => todo!(),
        };
        let c2 = match self.client2.clock(at_least).await {
            Ok(x) => x,
            Err(_) => todo!(),
        };
        let cmax = std::cmp::max(c1, c2);
        return Ok(cmax);
    }
}
fn ks_log_key_wrapper(s: &str) -> String {
    return "list#keystring#".to_string() + s;
}

fn ks_log_value_wrapper(v: &str, clock: u64) -> String {
    return clock.to_string() + SEPARATOR + v;
}
fn ks_log_value_unwrapper(s: &str) -> (String, u64) {
    let id = s.find(SEPARATOR).unwrap();
    let clock = s[0..id].parse::<u64>().unwrap();
    let v = s[id + 1..].to_string();
    return (v, clock);
}
#[async_trait] // VERY IMPORTANT !!
impl KeyString for VirBinStorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let composed_key = ks_log_key_wrapper(&self.wrap_with_user_name(key));
        let res1 = match self.client1.list_get(composed_key.as_str()).await {
            Ok(x) => x.0,
            Err(_) => todo!(),
        };
        let res2 = match self.client2.list_get(composed_key.as_str()).await {
            Ok(x) => x.0,
            Err(_) => todo!(),
        };

        let mut cmax = 0;
        let mut vret = None;
        res1.iter().for_each(|x| {
            let (v, c) = ks_log_value_unwrapper(x);
            if (c > cmax) {
                vret = Some(v);
                cmax = c;
            }
        });
        let mut vret = None;
        res2.iter().for_each(|x| {
            let (v, c) = ks_log_value_unwrapper(x);
            if (c > cmax) {
                vret = Some(v);
                cmax = c;
            }
        });
        return Ok(vret);
    }
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut kv_log = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_log.key.as_str());
        let c = self.clock(0).await?;
        kv_log.key = ks_log_key_wrapper(&composed_key);
        kv_log.value = ks_log_value_wrapper(&kv_log.value.as_str(), c);

        self.client1.list_append(&kv_log).await?;
        self.client2.list_append(&kv_log).await?;
        return Ok(true);
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let mut p_clone = p.clone();
        p_clone.prefix = ks_log_key_wrapper(&self.wrap_with_user_name(&p_clone.prefix));
        let l1 = self.client1.keys(&p_clone).await?;
        let res1: Vec<String> = l1.0.iter().map(|x| self.dewrap_with_user_name(x)).collect();
        let l2 = self.client1.keys(&p_clone).await?;
        let res2: Vec<String> = l2.0.iter().map(|x| self.dewrap_with_user_name(x)).collect();
        // let res = Set::new(res2);
        let mut res: HashSet<String> = res2.into_iter().collect();
        res1.iter().map(|x| {
            res.insert(x.clone());
        });
        return Ok(List {
            0: res.into_iter().collect(),
        });
    }
}

#[async_trait] // VERY IMPORTANT !!
impl KeyList for VirBinStorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        self.client1
            .list_get(self.wrap_with_user_name(key).as_str())
            .await
    }
    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut kv_clone = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_clone.key.as_str());
        kv_clone.key = composed_key;
        Ok(self.client1.list_append(&kv_clone).await?.clone())
    }
    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut kv_clone = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_clone.key.as_str());
        kv_clone.key = composed_key;
        self.client1.list_remove(&kv_clone).await
    }
    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let mut p_clone = p.clone();
        p_clone.prefix = self.wrap_with_user_name(&p_clone.prefix);
        let tmp = self.client1.list_keys(&p_clone).await?;
        let res_: Vec<String> = tmp
            .0
            .iter()
            .map(|x| self.dewrap_with_user_name(x))
            .collect();
        Ok(List { 0: res_ })
    }
}

// #[async_trait] // VERY IMPORTANT !!
// impl storage::KeyList for BinStorageClient {
//     async fn list_get(&self, key: &str) -> TribResult<List> {
//         let mut client = TribBinStorageClient::connect(self.addr.clone()).await?;
//         let r = client
//             .list_get(Key {
//                 key: key.to_string(),
//             })
//             .await?;
//         match r.into_inner().value {
//             value => Ok(value),
//         }
//     }

//     /// Append a string to the list. return true when no error.
//     async fn list_append(&self, kv: &KeyValue) -> TribResult<bool>;

//     /// Removes all elements that are equal to `kv.value` in list `kv.key`
//     /// returns the number of elements removed.
//     async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32>;

//     /// List all the keys of non-empty lists, where the key matches
//     /// the given pattern.
//     async fn list_keys(&self, p: &Pattern) -> TribResult<List>;
// }
