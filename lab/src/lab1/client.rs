use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::channel;
use tonic::transport::Endpoint;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::Key;
use tribbler::storage;
use tribbler::storage::KeyValue;
use tribbler::storage::List;

use tribbler::storage::Pattern;
use tribbler::storage::{KeyList, KeyString, Storage};
pub struct StorageClient {
    pub addr: String,
    pub channels: Vec<Mutex<channel::Channel>>,
}
#[async_trait]
pub trait Newhack {
    // async fn gget(&self, key: &str) -> TribResult<Option<String>>;

    async fn neww(addr: &str) -> StorageClient;
}
#[async_trait]
impl Newhack for StorageClient {
    async fn neww(addr: &str) -> StorageClient {
        let mut channels = vec![];
        for i in 0..10 {
            let ep = match Endpoint::from_shared(addr.to_string()) {
                Ok(x) => x,
                Err(e) => todo!(),
            };
            loop {
                let tmp = ep.connect();

                match tmp.await {
                    Ok(x) => {
                        channels.push(Mutex::new(x));
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }

        return StorageClient {
            channels: channels,
            addr: addr.to_string(),
        };
    }
}
#[async_trait] // VERY IMPORTANT !!
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        println!("{:?}", at_least);
        // todo!()
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .clock(rpc::Clock {
                                timestamp: at_least,
                            })
                            .await?;

                        return Ok(r.into_inner().timestamp as u64);
                        // {
                        //     value => Ok(value as u64),
                        // }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
}

#[async_trait] // VERY IMPORTANT !!
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        println!("{:?}", key);

        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .get(Key {
                                key: key.to_string(),
                            })
                            .await?;
                        let s = r.into_inner().value;
                        if !s.is_empty() {
                            return Ok(Some(s));
                        } else {
                            return Ok(None);
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        println!("{:?}", kv);

        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .set(tribbler::rpc::KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            })
                            .await?;

                        match r.into_inner().value {
                            value => return Ok(value),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        println!("{:?}", p);
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .keys(rpc::Pattern {
                                prefix: p.prefix.clone(),
                                suffix: p.suffix.clone(),
                            })
                            .await?;
                        match r.into_inner().list {
                            value => return Ok(List(value)),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
}
#[async_trait] // VERY IMPORTANT !!
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        println!("{:?}", key);
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .list_get(rpc::Key {
                                key: key.to_string(),
                            })
                            .await?;
                        match r.into_inner().list {
                            value => return Ok(List(value)),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        println!("{:?}", kv);
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .list_append(tribbler::rpc::KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            })
                            .await?;

                        match r.into_inner().value {
                            value => return Ok(value),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        println!("{:?}", kv);
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .list_remove(tribbler::rpc::KeyValue {
                                key: kv.key.clone(),
                                value: kv.value.clone(),
                            })
                            .await?;

                        match r.into_inner().removed {
                            value => return Ok(value),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        println!("{:?}", p);
        loop {
            for channel in &self.channels {
                match channel.try_lock() {
                    Ok(x) => {
                        let mut client = TribStorageClient::new(x.clone());
                        let r = client
                            .list_keys(rpc::Pattern {
                                prefix: p.prefix.clone(),
                                suffix: p.suffix.clone(),
                            })
                            .await?;
                        match r.into_inner().list {
                            value => return Ok(List(value)),
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
}

// #[async_trait] // VERY IMPORTANT !!
// impl storage::KeyList for StorageClient {
//     async fn list_get(&self, key: &str) -> TribResult<List> {
//         let mut client = TribStorageClient::connect(self.addr.clone()).await?;
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
