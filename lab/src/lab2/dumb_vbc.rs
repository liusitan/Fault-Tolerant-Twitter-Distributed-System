#![allow(unused_parens)]
use crate::lab1::client;
use std::collections::HashSet;
use std::mem::drop;
use std::sync::Arc;
// use crate::lab1::client::Newhack;
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;

use std::time;
use tokio::sync::RwLock;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::rpc::Key;
use tribbler::storage;
use tribbler::storage::KeyValue;
use tribbler::storage::List;
use tribbler::storage::Pattern;
use tribbler::storage::{KeyList, KeyString, Storage};

use super::utility::cons_hash;

#[derive(Debug)]
pub struct VirBinStorageClient {
    pub backs: Vec<String>,
    pub user_name: String,
    hashid: usize,
    pub client1: Arc<RwLock<Box<client::StorageClient>>>,
    pub client2: Arc<RwLock<Box<client::StorageClient>>>,
    clients_update_at: RwLock<time::Instant>,
}

const addr1: &str = "A1";
const addr2: &str = "A2";

const SEPARATOR: &str = "|";

impl VirBinStorageClient {
    pub async fn new(addrs: &Vec<String>, name: &str) -> TribResult<VirBinStorageClient> {
        let hashed_addrs: Vec<u64> = addrs
            .iter()
            .map(|x| cons_hash(&("http://".to_string() + x)))
            .collect();
        let mut my_addrs = addrs.clone();
        my_addrs.sort_by_key(|x| cons_hash(&("http://".to_string() + x)));
        let mut hid = hashed_addrs
            .binary_search(&cons_hash(&name.to_string()))
            .unwrap_or_else(|x| x % addrs.len());

        // let hid = calculate_hash(&(name.to_string())) as usize;
        return Ok(VirBinStorageClient {
            backs: my_addrs.to_owned(),
            hashid: hid,
            user_name: name.to_owned(),
            client1: Arc::new(RwLock::new(Box::new(
                client::StorageClient::new(&my_addrs[hid]).await?,
            ))),
            client2: Arc::new(RwLock::new(Box::new(
                client::StorageClient::new(&my_addrs[hid + 1]).await?,
            ))),
            clients_update_at: RwLock::new(time::Instant::now()),
        });
    }

    // async fn reconfigureclient1(&mut self) -> TribResult<()> {

    pub async fn reconfigureclients(
        &self,
    ) -> TribResult<(client::StorageClient, client::StorageClient)> {
        println!("reconfiguring clients");

        // println!("74");

        // match c1_write.get("").await {
        //     Ok(x) => (),
        //     Err(err) => println!("{:?}", err),
        // }
        let mut c1_candidate;
        let c2 = self.client2.clone();
        let mut c2_write = c2.write().await;
        let mut c2_candidate;
        let mut id = self.hashid;

        loop {
            match client::StorageClient::new(&self.backs[id]).await {
                Ok(x) => {
                    println!("generate client1 {}", id);
                    id += 1;
                    c1_candidate = x;
                    break;
                }
                Err(_) => {
                    id = id + 1;
                    continue;
                }
            }
        }
        // match c1_write.get("").await {
        //     Ok(x) => (),
        //     Err(err) => println!("{:?}", err),
        // }

        loop {
            match client::StorageClient::new(&self.backs[id]).await {
                Ok(x) => {
                    println!("generate client2  id{}", id);
                    c2_candidate = x;
                    id += 1;

                    break;
                }
                Err(_) => {
                    id = id + 1;
                    continue;
                }
            }
        }

        return Ok((c1_candidate, c2_candidate));
    }

    fn wrap_with_user_name(&self, s: &str) -> String {
        //$$println!("wrapped name {:?}", self.user_name.clone() + "|" + s);
        "bin#".to_string() + &self.user_name + SEPARATOR + s
    }
    fn unwrap_with_user_name(&self, s: &str) -> String {
        let idx = s.find(SEPARATOR).unwrap();
        return s[(idx + 1)..].to_string();
    }
}

#[async_trait] // VERY IMPORTANT !!
impl Storage for VirBinStorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        // let mut unlock_1 = self.client1.read().await;
        // let mut unlock_2 = self.client2.read().await; //.map_err(|e| e.to_string())?;
        //
        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();
        let mut c1 = match unlock_1.clock(at_least).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;
                unlock_1.clock(0).await? // let unlock_mut = unlock_2.as_mut();
            }
        };
        let mut c2 = match unlock_2.clock(at_least).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;
                unlock_2.clock(c1).await? // let unlock_mut = unlock_2.as_mut();
                                          // let unlock_mut = unlock_2.as_mut();
            }
        };
        let mut cmax = std::cmp::max(c1, c2);
        cmax = match unlock_1.clock(cmax).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.clock(cmax).await?
            }
        };
        cmax = match unlock_1.clock(cmax).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.clock(cmax).await?
            }
        };
        let c2 = match unlock_2.clock(cmax).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.clock(cmax).await?
            }
        };
        let cmax = std::cmp::max(c1, c2);
        println!("clock ends");

        return Ok(cmax);
    }
}
fn ks_log_key_wrapper(s: &str) -> String {
    return "log#keystring#".to_string() + SEPARATOR + s;
}
fn ks_log_key_unwrapper(s: &str) -> String {
    let id = s.find(SEPARATOR).unwrap();
    let k = s[id + 1..].to_string();
    return k;
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
        println!("get starts");

        self.reconfigureclients().await?;

        let composed_key = self.wrap_with_user_name(&ks_log_key_wrapper(key));

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        let res1 = match unlock_1.list_get(composed_key.as_str()).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_get(composed_key.as_str()).await?.0
            }
        };
        println!("c1list: {:?}", res1);

        let res2 = match unlock_2.list_get(composed_key.as_str()).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_get(composed_key.as_str()).await?.0
            }
        };
        println!("c2list: {:?}", res2);

        println!("finished two request");
        let mut cmax = 0;
        let mut vret = None;
        res1.iter().for_each(|x| {
            let (v, c) = ks_log_value_unwrapper(x);
            if (c > cmax) {
                if (v.eq("")) {
                    vret = None;
                } else {
                    vret = Some(v);
                }
                cmax = c;
            }
        });
        // println!("{:?} cmax:{}", vret, cmax);
        res2.iter().for_each(|x| {
            let (v, c) = ks_log_value_unwrapper(x);
            if (c > cmax) {
                // println!("{}", c);
                if (v.eq("")) {
                    vret = None;
                } else {
                    vret = Some(v);
                }
                cmax = c;
            }
        });
        // println!("{:?}", vret);
        println!("get ends");

        return Ok(vret);
    }
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        println!("set starts");

        self.reconfigureclients().await?;

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        // println!("ssssssssss{:?}", kv);
        let mut kv_log = kv.clone();
        // let composed_key = );
        let c = self.clock(0).await?;
        kv_log.key = self.wrap_with_user_name(&ks_log_key_wrapper(kv_log.key.as_str()));
        kv_log.value = ks_log_value_wrapper(&kv_log.value.as_str(), c);

        match unlock_1.list_append(&kv_log).await {
            // Ok(x) => (println!("{}", x)),
            Ok(_) => (),
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_append(&kv_log).await?;
            }
        };
        match unlock_2.list_append(&kv_log).await {
            // Ok(x) => (println!("{}", x)),
            Ok(_) => (),
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_append(&kv_log).await?;
            }
        };
        println!("set ends");

        return Ok(true);
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        self.reconfigureclients().await;

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        // println!("kkkkkkkk");
        let mut p_clone = p.clone();
        p_clone.prefix = self.wrap_with_user_name(&ks_log_key_wrapper(&p_clone.prefix));
        let l1 = match unlock_1.list_keys(&p_clone).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_keys(&p_clone).await?.0
            }
        };
        let res1: Vec<String> = l1
            .iter()
            .map(|x| ks_log_key_unwrapper(&self.unwrap_with_user_name(x)))
            .collect();
        // println!("length:{} res1: {:?}", l1.0.len(), res1);
        let l2 = match unlock_2.list_keys(&p_clone).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_keys(&p_clone).await?.0
            }
        };
        let res2: Vec<String> = l2
            .iter()
            .map(|x| ks_log_key_unwrapper(&self.unwrap_with_user_name(x)))
            .collect();
        // println!("length:{} res2: {:?}", l2.0.len(), res2);
        let mut res: HashSet<String> = res2.clone().into_iter().collect();
        res1.iter().for_each(|x| {
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
        self.reconfigureclients().await;

        // println!("llllllllllll");
        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        let wrappedkey = self.wrap_with_user_name(&kl_log_key_wrapper(key));
        let mut c1list: Vec<ListOpLog> = match unlock_1.list_get(&wrappedkey).await {
            Ok(x) => {
                x.0.iter()
                    .map(|x| {
                        let r: ListOpLog = serde_json::from_str(x).expect("ListOpLog");
                        return r;
                    })
                    .collect()
            }
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1
                    .list_get(&wrappedkey)
                    .await?
                    .0
                    .iter()
                    .map(|x| {
                        let r: ListOpLog = serde_json::from_str(x).expect("ListOpLog");
                        return r;
                    })
                    .collect()
            }
        };
        println!("c1list: {:?}", c1list);
        let mut c2list: Vec<ListOpLog> = match unlock_2.list_get(&wrappedkey).await {
            Ok(x) => {
                x.0.iter()
                    .map(|x| {
                        let r: ListOpLog = serde_json::from_str(x).expect("ListOpLog");
                        return r;
                    })
                    .collect()
            }
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2
                    .list_get(&wrappedkey)
                    .await?
                    .0
                    .iter()
                    .map(|x| {
                        let r: ListOpLog = serde_json::from_str(x).expect("ListOpLog");
                        return r;
                    })
                    .collect()
            }
        };
        println!("c2list: {:?}", c2list);

        c1list.sort_by_key(|x| x.clock);
        c2list.sort_by_key(|x| x.clock);
        let merged = merge_two_list(&c1list, &c2list);
        return Ok(List {
            0: compute_current_list(&merged),
        });
    }
    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        self.reconfigureclients().await;

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        let c = self.clock(0).await?;
        let wrappedkey = self.wrap_with_user_name(&kl_log_key_wrapper(&kv.key));
        let value = ListOpLog {
            op: ListOp::Append,
            clock: c,
            value: kv.value.clone(),
        };
        let kv_send = KeyValue {
            key: wrappedkey,
            value: serde_json::to_string(&value)?,
        };
        let c1 = match unlock_1.list_append(&kv_send).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_append(&kv_send).await?
            }
        };
        let c2 = match unlock_2.list_append(&kv_send).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_append(&kv_send).await?
            }
        };
        return Ok(true);
    }
    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        self.reconfigureclients().await;

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        let c = self.clock(0).await?;
        let wrappedkey = self.wrap_with_user_name(&kl_log_key_wrapper(&kv.key));
        let value = ListOpLog {
            op: ListOp::Remove,
            clock: c,
            value: kv.value.clone(),
        };
        let kv_send = KeyValue {
            key: wrappedkey,
            value: serde_json::to_string(&value)?,
        };
        let l1 = self.list_get(&kv.key).await?;
        let mut remove_num = 0;
        l1.0.iter().for_each(|x| {
            if (x.eq(&kv.value)) {
                remove_num += 1;
            }
        });
        let c1 = match unlock_1.list_append(&kv_send).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_append(&kv_send).await?
            }
        };
        let c2 = match unlock_2.list_append(&kv_send).await {
            Ok(x) => x,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_append(&kv_send).await?
            }
        };
        return Ok(remove_num);
    }
    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        // println!("xxxxxxxxxxxxxx");
        self.reconfigureclients().await;

        let (mut unlock_1, mut unlock_2) = self.reconfigureclients().await?; // let unlock_mut = unlock_2.as_mut();

        let mut p_clone = p.clone();
        p_clone.prefix = self.wrap_with_user_name(&kl_log_key_wrapper(&p_clone.prefix));
        let k1 = match unlock_1.list_keys(&p_clone).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_1.list_keys(&p_clone).await?.0
            }
        };
        // println!("xxxxxxx{:?}", k1);
        let k2 = match unlock_2.list_keys(&p_clone).await {
            Ok(x) => x.0,
            Err(_) => {
                (unlock_1, unlock_2) = self.reconfigureclients().await?;

                unlock_2.list_keys(&p_clone).await?.0
            }
        };
        // println!("xxxxxxxx{:?}", k2);

        let mut res: HashSet<String> = k1
            .iter()
            .map(|x| {
                return kl_log_key_unwrapper(&self.unwrap_with_user_name(x));
            })
            .collect();
        k2.iter().for_each(|x| {
            res.insert(kl_log_key_unwrapper(&self.unwrap_with_user_name(x)));
        });
        return Ok(List {
            0: res.iter().map(|x| x.clone()).collect(),
        });
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListOpLog {
    op: ListOp,
    clock: u64,
    value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ListRemoveLog {
    clock: u64,
    value: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]

enum ListOp {
    Append,
    Remove,
}

fn kl_log_key_wrapper(s: &str) -> String {
    return "log#keylist#".to_string() + SEPARATOR + s;
}
fn kl_log_key_unwrapper(s: &str) -> String {
    let id = s.find(SEPARATOR).unwrap();
    let k = s[id + 1..].to_string();
    return k;
}
fn merge_two_list(l1: &Vec<ListOpLog>, l2: &Vec<ListOpLog>) -> Vec<ListOpLog> {
    let mut res = vec![];
    let length1 = l1.len();
    if length1 == 0 {
        return l2.clone();
    }
    let length2 = l2.len();
    if length2 == 0 {
        return l1.clone();
    }
    let mut i1 = 0;
    let mut i2 = 0; //

    while (i1 < length1 || i2 < length2) {
        if (i1 == length1) {
            res.push(l1[i1].clone());
            i2 += 1;
            continue;
        }
        if (i2 == length2) {
            res.push(l2[i2].clone());
            i1 += 1;
            continue;
        }
        let e1 = l1[i1].clone();
        let e2 = l2[i2].clone();
        if (e1.clock == e2.clock) {
            res.push(e1.clone());
            i1 += 1;
            i2 += 1;
        } else if (e1.clock < e2.clock) {
            res.push(e1.clone());
            i1 += 1;
        } else {
            res.push(e2.clone());
            i2 += 1;
        }
    }
    return res;
}
fn compute_current_list(l: &Vec<ListOpLog>) -> Vec<String> {
    let mut res: Vec<String> = vec![];
    let mut s: HashSet<u64> = vec![].into_iter().collect();
    // println!("{:?}", l);
    l.iter().for_each(|x| {
        let log = x;
        if (!s.contains(&log.clock)) {
            match x.op.clone() {
                ListOp::Append => {
                    res.push(log.value.clone());
                }
                ListOp::Remove => {
                    res = res
                        .iter()
                        .filter(|x| !(*x).eq(&log.value))
                        .map(|x| x.clone())
                        .collect();
                }
            }
            s.insert(log.clock);
        }
    });
    // println!("{:?}", res);
    return res;
}

// fn kl_log_kv_wrapper(lr: &ListRemoveLog, clock: u64) -> TribResult<String> {
//     return Ok(clock.to_string() + SEPARATOR + serde_json::to_string(kv));
// }
// fn kl_log_kv_unwrapper(s: &str) -> TribResult<(KeyValue, u64)> {
//     let id = s.find(SEPARATOR).unwrap();
//     let c = s[0..id].parse::<u64>();
//     let kv: KeyValue = serde_json::from_str(s)?;
//     return Ok((c, kv));
// }
