 
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{KeyValue, Pattern, Value};

pub struct KeeperServer {
    pub keeper_addr: String,
    pub backends: Vec<String>,
    pub hashed_backends: Vec<i32>,
    pub assigned: Vec<String>,
    pub liveliness: Vec<bool>,
}

pub const MIGRATE_TASK_LOG: &str = "migrate_task_log";
pub const MIGRATE_START_MSG: &str = "start";
pub const MIGRATE_END_MSG: &str = "end";

fn cons_hash(val: &String) {

}

impl KeeperServer {

    async fn check_liveliness(&self, backend: &String) -> bool {
        let conn = TribStorageClient::connect(backend).await;
        if !conn.is_ok() {
            return false;
        } else {
            return true;
        }
    }

    async fn get_primary_backend(&self, mut idx: usize, key: &String) -> usize {
        loop {
            let backend = self.backends[idx];
            let conn = TribStorageClient::connect(backend).await;
            if conn.is_ok() {
                let client = conn.unwrap();
                let string_key_resp = client.get(key).unwrap();
                if !string_key_resp.is_none() {
                    return idx;
                }
                let list_key = client.list_get(key).unwrap();
                if !list_key.0.is_empty() {
                    return idx;
                }
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn get_next_replica_backend(&self, mut idx: usize, key: &String) -> usize {
        idx = (idx + 1) % self.backends.len();
        loop {
            let backend = self.backends[idx];
            let conn = TribStorageClient::connect(backend).await;
            if conn.is_ok() {
                let client = conn.unwrap();
                let string_key_resp = client.get(key).unwrap();
                if !string_key_resp.is_none() {
                    return idx;
                }
                let list_key = client.list_get(key).unwrap();
                if !list_key.0.is_empty() {
                    return idx;
                }
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn get_next_living_backend(&self, mut idx: usize) -> usize {
        idx = (idx + 1) % self.backends.len();
        loop {
            let backend = self.backends[idx];
            let conn = TribStorageClient::connect(backend).await;
            if conn.is_ok() {
                return idx;
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn find_predecessor(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self.hashed_backends.binary_search(cons_hash(failed_backend)).unwrap_or_else(|x| x);
        srv_idx = (srv_idx - 1) % self.backends.len();
        loop {
            let backend = self.backends[idx];
            let conn = TribStorageClient::connect(backend).await;
            if conn.is_ok() {
                return backend.clone();
            }
            srv_idx = (srv_idx - 1) % self.backends.len();
        }
    }

    async fn find_succesor(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self.hashed_backends.binary_search(cons_hash(failed_backend)).unwrap_or_else(|x| x);
        let next_srv = self.get_next_living_backend(srv_idx).await;
        return self.backends[next_srv].clone();
    }

    async fn get_keys(&self, srv: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv).await;
        if conn.is_ok() {
            let client = conn.unwrap();
            let keys_resp = client.keys(Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            }).await;
            if keys_resp.is_ok() {
                let keys = keys_resp.unwrap();
                if !keys.is_none {
                    return keys.unwrap();
                }
            }
        }
        return Vec::new();
    }

    async fn get_list_keys(&self, srv: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv).await;
        if conn.is_ok() {
            let client = conn.unwrap();
            let keys_resp = client.list_keys(Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            }).await;
            if keys_resp.is_ok() {
                return keys_resp.unwrap();
            }
        }
        return Vec::new();
    }

    async fn start_migration(&self, failed_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = !format("{}:{}", MIGRATE_TASK_LOG, failed_backend);
        let log_srv_idx = self.hashed_backends.binary_search(log_entry_key).unwrap_or_else(|x| x);
        let log_srv_primary = self.backends[log_srv_idx];
        let log_srv_replica = self.get_next_replica_backend(log_srv_idx, log_entry_key).await;

        let conn_primary = TribStorageClient::connect(log_srv_primary).await;
        if conn_primary.is_ok() {
            let client = conn.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.to_string(),
                value: MIGRATE_START_MSG
            });
        }

        let conn_replica = TribStorageClient::connect(log_srv_replica).await;
        if conn_replica.is_ok() {
            let client = conn.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.to_string(),
                value: MIGRATE_START_MSG
            });
        }
    }

    async fn end_migration(&self, failed_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = !format("{}:{}", MIGRATE_TASK_LOG, failed_backend);
        let log_srv_idx = self.hashed_backends.binary_search(log_entry_key).unwrap_or_else(|x| x);
        let log_srv_primary = self.backends[log_srv_idx];
        let log_srv_replica = self.get_next_replica_backend(log_srv_idx, log_entry_key).await;

        let conn_primary = TribStorageClient::connect(log_srv_primary).await;
        if conn_primary.is_ok() {
            let client = conn.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.to_string(),
                value: MIGRATE_START_MSG
            });
        }

        let conn_replica = TribStorageClient::connect(log_srv_replica).await;
        if conn_replica.is_ok() {
            let client = conn.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.to_string(),
                value: MIGRATE_START_MSG
            });
        }
    }

    async fn get_key_string_value(&self, srv: &String, key: &String) -> Option<String> {
        let conn = TribStorageClient::connect(srv).await;
        if conn.is_ok() {
            let client = conn.unwrap();
            let resp = client.get(Key {
                key: key
            }).await;
            if resp.is_ok() {
                return Some(resp.unwrap().into_inner().value.clone());
            }
        }
        return None;
    }

    async fn get_key_list_value(&self, srv: &String, key: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv).await;
        if conn.is_ok() {
            let client = conn.unwrap();
            let resp = client.list_get(Key {
                key: key
            }).await;
            if resp.is_ok() {
                return resp.unwrap().into_inner().0;
            }
        }
        return Vec::new();
    }

    // consider write log for starting migration for failed backend first
    // assuming no backend fails after a failure
    async fn migrate(&self, failed_backend: &String) -> Result<()> {
        // use write-ahead log to tolerate keeper fault
        self.start_migration().await;

        let prev_srv = self.find_predecessor(failed_backend);
        let next_srv = self.find_succesor(failed_backend);

    
        // get the keys from prev_srv and next_srv
        let mut string_keys_prev_srv = Vec::new();
        let mut list_keys_prev_srv = Vec::new();
        string_keys_prev_srv.extend(self.get_keys(&prev_srv));
        list_keys_prev_srv.extend(self.list_keys(&prev_srv));
        let mut string_keys_succ_srv = Vec::new();
        let mut list_keys_succ_srv = Vec::new();
        string_keys_succ_srv.extend(self.get_keys(&next_srv));
        list_keys_succ_srv.extend(self.list_keys(&next_srv));

        // do successor migration first
        for &string_key in string_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self.hashed_backends.binary_search(string_key).unwrap_or_else(|x| x);
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = self.backends[primary_idx];
            if primary_srv.eq(prev_srv) {
                // primary key at primary srv, make a copy
                let val = self.get_key_string_value(primary_srv, string_key).await;
                self.migrate_keyval(key, val.unwrap(), );
            }

            let replica_idx = self.get_next_replica_backend(primary_idx, string_key).await;
            let available_idx = self.get_next_living_backend(replica_idx).await;
        }

        for &string_key in string_keys_succ_srv.iter() {
            
        }


        // from the keys of previous server's,
        // identify whether it is primary or backup
        
        for &string_key in string_keys_prev_srv.iter() {
            // binary search the key hash
            let insert_idx = self.hashed_backends.binary_search(string_key).unwrap_or_else(|x| x);
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = self.backends[primary_idx];
            if primary_srv.eq(prev_srv) {
                // primary key at primary srv, make a copy
                let val = self.get_key_string_value(primary_srv, string_key).await;
                self.migrate_keyval(key, val.unwrap(), );
            }

            let replica_idx = self.get_next_replica_backend(primary_idx, string_key).await;
            let available_idx = self.get_next_living_backend(replica_idx).await;

            // copy the key to 
        }

        // loop string list and migrate
        for &list_key in list_keys_prev_srv.iter() {

        }

        self.end_migration().await;
    }


    async fn fill(recover_backend: &String) -> Result<()> {

    }
}