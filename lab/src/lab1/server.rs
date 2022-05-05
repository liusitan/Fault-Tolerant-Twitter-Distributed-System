use async_trait::async_trait;
use tonic;
use tribbler::rpc;
use tribbler::rpc::trib_storage_server::TribStorage;
use tribbler::storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage};
pub struct MyStorageServer {
    ram_storage: Box<dyn Storage>,
}
impl MyStorageServer {
    /// Creates a new instance of [MemStorage]
    pub fn new(b: Box<dyn Storage>) -> MyStorageServer {
        MyStorageServer { ram_storage: b }
    }
}

#[async_trait] // VERY IMPORTANT !!
impl TribStorage for MyStorageServer {
    async fn get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        println!("Key = {:?}", request);
        let key = request.get_ref();
        let v = self.ram_storage.get(key.key.as_str()).await;
        match v {
            Ok(x) => match x {
                Some(y) => Ok(tonic::Response::new(rpc::Value { value: y })),
                None => Ok(tonic::Response::new(rpc::Value {
                    value: "".to_string(),
                })),
            },
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        println!("Key-value = {:?}", request);
        let kv = request.get_ref();
        let v = self
            .ram_storage
            .set(&KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::Bool { value: x })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        println!("patterns = {:?}", request);
        let p = request.get_ref();
        let v = self
            .ram_storage
            .keys(&Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::StringList { list: x.0 })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        println!("list_get = {:?}", request);
        let k = request.get_ref();
        let v = self.ram_storage.list_get(k.key.as_str()).await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::StringList { list: x.0 })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        println!("list_append = {:?}", request);
        let kv = request.get_ref();
        let v = self
            .ram_storage
            .list_append(&KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::Bool { value: x })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        println!("list_remove = {:?}", request);
        let kv = request.get_ref();
        let v = self
            .ram_storage
            .list_remove(&KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::ListRemoveResponse { removed: x })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        println!("list_keys = {:?}", request);
        let p = request.get_ref();
        let v = self
            .ram_storage
            .list_keys(&Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::StringList { list: x.0 })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        println!("clocks = {:?}", request);
        let c = request.get_ref();
        let v = self.ram_storage.clock(c.timestamp).await;
        match v {
            Ok(x) => Ok(tonic::Response::new(rpc::Clock { timestamp: x })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
}
