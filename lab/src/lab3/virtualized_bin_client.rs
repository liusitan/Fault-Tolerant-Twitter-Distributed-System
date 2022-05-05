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
    pub client: client::StorageClient,
}
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
            client: <client::StorageClient as client::Newhack>::neww(addr).await,
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
        self.client.clock(at_least).await
    }
}

#[async_trait] // VERY IMPORTANT !!
impl KeyString for VirBinStorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let composed_key = self.wrap_with_user_name(key);
        self.client.get(composed_key.as_str()).await
    }
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut kv_clone = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_clone.key.as_str());
        kv_clone.key = composed_key;
        self.client.set(&kv_clone).await
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let mut p_clone = p.clone();
        p_clone.prefix = self.wrap_with_user_name(&p_clone.prefix);
        let tmp = self.client.keys(&p_clone).await?;
        let res_: Vec<String> = tmp
            .0
            .iter()
            .map(|x| self.dewrap_with_user_name(x))
            .collect();
        Ok(List { 0: res_ })
    }
}
#[async_trait] // VERY IMPORTANT !!
impl KeyList for VirBinStorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        self.client
            .list_get(self.wrap_with_user_name(key).as_str())
            .await
    }
    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut kv_clone = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_clone.key.as_str());
        kv_clone.key = composed_key;
        Ok(self.client.list_append(&kv_clone).await?.clone())
    }
    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut kv_clone = kv.clone();
        let composed_key = self.wrap_with_user_name(kv_clone.key.as_str());
        kv_clone.key = composed_key;
        self.client.list_remove(&kv_clone).await
    }
    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let mut p_clone = p.clone();
        p_clone.prefix = self.wrap_with_user_name(&p_clone.prefix);
        let tmp = self.client.list_keys(&p_clone).await?;
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
