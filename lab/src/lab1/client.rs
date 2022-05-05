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
#[derive(Clone, Debug)]

pub struct StorageClient {
    pub addr: String,
    pub channel: channel::Channel,
}

impl StorageClient {
    pub async fn new(addr: &str) -> TribResult<StorageClient> {
        let channel = Endpoint::from_shared(addr.to_string())?.connect().await?;

        return Ok(StorageClient {
            channel: channel,
            addr: addr.to_string(),
        });
    }
}
#[async_trait] // VERY IMPORTANT !!
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
        let r = client
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;

        return Ok(r.into_inner().timestamp as u64);
    }
}

#[async_trait] // VERY IMPORTANT !!
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);

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

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
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

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
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
}
#[async_trait] // VERY IMPORTANT !!
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        println!("{:?}", key);
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
        let r = client
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        match r.into_inner().list {
            value => return Ok(List(value)),
        }
    }
    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
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
    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        println!("{:?}", kv);
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
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
    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let channel_clone = self.channel.clone();
        let mut client = TribStorageClient::new(channel_clone);
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
