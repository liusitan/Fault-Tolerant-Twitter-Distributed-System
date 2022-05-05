use tribbler::rpc;
use tribbler::rpc::trib_storage_server as rs;
use tribbler::storage;
use tribbler::storage::Storage;

pub struct StorageServer {
    pub storage: Box<dyn Storage>,
}

const debug: bool = false;

// The four functions below are to return special error message that can be treated differently in client
pub fn defaultErrStatus() -> tonic::Status {
    return tonic::Status::new(tonic::Code::Unknown, format!("Default error message"));
}

pub fn errStatusWithStr(str: String) -> tonic::Status {
    return tonic::Status::new(tonic::Code::Unknown, str);
}

pub fn errPass() -> tonic::Status {
    return tonic::Status::new(tonic::Code::Unknown, strPass);
}
pub fn errFail() -> tonic::Status {
    return tonic::Status::new(tonic::Code::Unknown, strFail);
}

pub const strPass: &str = "Pass";

pub const strFail: &str = "Fail";

#[async_trait::async_trait]
impl rs::TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        if debug {
            println!("Debug: server get() now having request: {:?}", request);
        }

        let request_value = request.into_inner();
        // QUESTION: how to make `?` work. Answer: probably it can't work here
        let store_result = (*self.storage).get(request_value.key.as_str()).await;
        let mut reply = rpc::Value {
            value: "".to_string(),
        };
        match store_result {
            Ok(inner_result) => match inner_result {
                Some(result) => reply = rpc::Value { value: result },
                None => return Err(errPass()),
            },
            Err(e) => return Err(errFail()),
        }
        if debug {
            println!(
                "Debug: server about to return from get()\n We have reply {:?}",
                reply
            );
        }
        return Ok(tonic::Response::new(reply));
    }
    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        if debug {
            println!("Debug: server set() now having request: {:?}", request);
        }

        let request_value = request.into_inner();
        let kv = storage::KeyValue::new(request_value.key.as_str(), request_value.value.as_str());
        let store_result = (*self.storage).set(&kv).await;
        let mut reply = rpc::Bool { value: false };
        match store_result {
            Ok(inner_result) => reply.value = inner_result,
            Err(e) => return Err(errFail()),
        }
        return Ok(tonic::Response::new(reply));
    }
    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        if debug {
            println!("Debug: server keys() now having request: {:?}", request);
        }

        let request_value = request.into_inner();
        let pattern = storage::Pattern {
            prefix: request_value.prefix,
            suffix: request_value.suffix,
        };
        let store_result = (*self.storage).keys(&pattern).await;
        let mut str_list: Vec<String> = Vec::new();
        match store_result {
            Ok(inner_result) => {
                for str in inner_result.0.iter() {
                    str_list.push((*str).clone());
                }
            }
            Err(e) => return Err(defaultErrStatus()),
        }
        let reply = rpc::StringList { list: str_list };
        return Ok(tonic::Response::new(reply));
    }

    // QUESTION: ask about my understanding of list_get Answer: seems to be correct
    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        if debug {
            println!("Debug: server list_get() now having request: {:?}", request);
        }

        let request_value = request.into_inner();
        let store_result = (*self.storage).list_get(request_value.key.as_str()).await;
        let mut str_list: Vec<String> = Vec::new();

        match store_result {
            Ok(inner_result) => {
                for str in inner_result.0.iter() {
                    str_list.push((*str).clone());
                }
            }
            Err(e) => return Err(defaultErrStatus()),
        }
        let reply = rpc::StringList { list: str_list };
        return Ok(tonic::Response::new(reply));
    }

    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        if debug {
            println!(
                "Debug: server list_append() now having request: {:?}",
                request
            );
        }

        let request_value = request.into_inner();
        let kv = storage::KeyValue::new(request_value.key.as_str(), request_value.value.as_str());
        let store_result = (*self.storage).list_append(&kv).await;
        let mut reply = rpc::Bool { value: false };
        match store_result {
            Ok(inner_result) => reply.value = inner_result,
            Err(e) => return Err(defaultErrStatus()),
        }
        return Ok(tonic::Response::new(reply));
    }
    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        if debug {
            println!(
                "Debug: server list_remove() now having request: {:?}",
                request
            );
        }

        let request_value = request.into_inner();
        let kv = storage::KeyValue::new(request_value.key.as_str(), request_value.value.as_str());
        let store_result = (*self.storage).list_remove(&kv).await;
        let mut reply = rpc::ListRemoveResponse { removed: 0 };
        match store_result {
            Ok(inner_result) => reply.removed = inner_result,
            Err(e) => return Err(defaultErrStatus()),
        }
        return Ok(tonic::Response::new(reply));
    }
    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        if debug {
            println!(
                "Debug: server list_keys() now having request: {:?}",
                request
            );
        }

        let request_value = request.into_inner();
        let pattern = storage::Pattern {
            prefix: request_value.prefix,
            suffix: request_value.suffix,
        };
        let store_result = (*self.storage).list_keys(&pattern).await;
        let mut str_list: Vec<String> = Vec::new();
        match store_result {
            Ok(inner_result) => {
                for str in inner_result.0.iter() {
                    str_list.push((*str).clone());
                }
            }
            Err(e) => return Err(defaultErrStatus()),
        }
        let reply = rpc::StringList { list: str_list };
        return Ok(tonic::Response::new(reply));
    }
    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        if debug {
            println!("Debug: server Clock() now having request: {:?}", request);
        }

        let request_value = request.into_inner();
        let store_result = (*self.storage).clock(request_value.timestamp.clone()).await;
        let mut reply = rpc::Clock { timestamp: 0 };
        match store_result {
            Ok(inner_result) => reply.timestamp = inner_result,
            Err(e) => return Err(defaultErrStatus()),
        }
        return Ok(tonic::Response::new(reply));
    }
}
