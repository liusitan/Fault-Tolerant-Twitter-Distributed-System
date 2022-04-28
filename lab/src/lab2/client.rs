use async_trait::async_trait;
use tribbler::rpc;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::{
    err::TribResult, storage::KeyList, storage::KeyString, storage::KeyValue, storage::List,
    storage::Pattern, storage::Storage,
};

use super::server::strPass;
use std::{thread, time::Duration};

pub struct StorageClient {
    pub addr: String,
    pub cl: Option<TribStorageClient<tonic::transport::Channel>>,
}

#[async_trait] // VERY IMPORTANT !!
impl KeyString for StorageClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e)); // TODO: how to write `if client == nil`
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .get(rpc::Key {
                key: key.to_string(),
            })
            .await;
        match r {
            Ok(v) => return Ok(Some(v.into_inner().value)),
            Err(status) => {
                if status.message() == strPass {
                    return Ok(None);
                } else {
                    return Err(Box::new(status));
                }
            }
        }
    }
    ///TODO: modify the rest of client
    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // let mut client = match &self.cl {
        //     Some(innerClient) => innerClient, // QUESTION: this compile error
        //     None => TribStorageClient::connect(self.addr.clone()).await?,
        // };
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e)); // TODO: how to write `if client == nil`
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;

        let r = client
            .set(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await;
        match r {
            Ok(_v) => return Ok(true),
            Err(status) => {
                if status.message() == strPass {
                    return Ok(false);
                } else {
                    return Err(Box::new(status));
                }
            }
        } // QUESTION: is it OK to just return, given we have await? above
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        let inner = r.into_inner();
        let mut list: Vec<String> = Vec::new();
        for str in inner.list {
            list.push(str);
        }
        return Ok(List(list));
    }
}

#[async_trait]
impl KeyList for StorageClient {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        let inner = r.into_inner();
        let mut list: Vec<String> = Vec::new();
        for str in inner.list.iter() {
            list.push((*str).clone());
        }
        return Ok(List(list));
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_append(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        return Ok(r.into_inner().value);
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_remove(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        return Ok(r.into_inner().removed);
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client;
        let mut count = 0;
        loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    client = cl;
                    break;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        }
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        let inner = r.into_inner();
        let mut list: Vec<String> = Vec::new();
        for str in inner.list.iter() {
            list.push((*str).clone());
        }
        return Ok(List(list));
    }
}

#[async_trait]
impl Storage for StorageClient {
    /// Returns an auto-incrementing clock. The returned value of each call will
    /// be unique, no smaller than `at_least`, and strictly larger than the
    /// value returned last time, unless it was [u64::MAX]

    /// QUESTION: what if I call `clock 200`, `clock 200`, and then `clock 201`? Is it OK to pass at_least as Clock.timestamp?
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        // code of naive
        // let mut client = TribStorageClient::connect(self.addr.clone()).await?;

        // code of storing client
        // let client = match &self.cl {
        //     Some(cl) => cl,
        //     None => TribStorageClient::connect(self.addr.clone()).await?,
        // }

        // TODO: read about c: opt<connection> = Mutex:: new(type), and whenever you use type, rust forces you to call c.lock().await

        // code of busy loop
        let mut count = 0;
        let mut client = loop {
            count += 1;
            match TribStorageClient::connect(self.addr.clone()).await {
                Ok(cl) => {
                    break cl;
                }
                Err(e) => {
                    thread::sleep(Duration::from_millis(30));
                    if count >= 100 {
                        return Err(Box::new(e));
                    } else {
                        continue;
                    }
                }
            }
        };

        let r = client
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        return Ok(r.into_inner().timestamp);
    }
}
