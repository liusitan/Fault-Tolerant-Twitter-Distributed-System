use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{Key, KeyValue, Pattern};

pub struct KeeperServer {
    pub keeper_addr: String,
    pub backends: Vec<String>,
    pub hashed_backends: Vec<i32>,
    pub assigned: Vec<String>,
    pub liveliness: Vec<bool>,
}

pub const MIGRATE_TASK_LOG: &str = "migrate_task_log";
pub const KEEPER_LOG: &str = "keeper_log";
pub const FAIL_TASK: &str = "fail";
pub const REC_TASK: &str = "recover";
pub const START_MSG: &str = "start";
pub const END_MSG: &str = "end";

fn cons_hash(val: &String) -> i32 {
    todo!();
}

impl KeeperServer {
    async fn check_liveliness(&self, backend: &String) -> bool {
        let conn = TribStorageClient::connect(backend.clone()).await;
        if !conn.is_ok() {
            return false;
        } else {
            return true;
        }
    }

    async fn get_primary_backend(&self, mut idx: usize, key: &String) -> usize {
        loop {
            let backend = &self.backends[idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                let mut client = conn.unwrap();
                let string_key_resp = client.get(Key { key: key.clone() }).await;
                if string_key_resp.is_ok() {
                    return idx;
                }
                let list_key_resp = client.list_get(Key { key: key.clone() }).await;
                if list_key_resp.is_ok() {
                    if !list_key_resp.unwrap().into_inner().list.is_empty() {
                        return idx;
                    }
                }
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn is_failed_backend_primary(&self, mut insert_idx: usize, failed_idx: usize) -> bool {
        loop {
            if insert_idx == failed_idx {
                return true;
            }
            let backend = &self.backends[insert_idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return false;
            }
            insert_idx = (insert_idx + 1) % self.backends.len();
        }
    }

    async fn get_next_replica_backend(&self, mut idx: usize, key: &String) -> usize {
        idx = (idx + 1) % self.backends.len();
        loop {
            let backend = &self.backends[idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                let mut client = conn.unwrap();
                let string_key_resp = client.get(Key { key: key.clone() }).await;
                if string_key_resp.is_ok() {
                    return idx;
                }
                let list_key_resp = client.list_get(Key { key: key.clone() }).await;
                if list_key_resp.is_ok() {
                    if !list_key_resp.unwrap().into_inner().list.is_empty() {
                        return idx;
                    }
                }
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn get_next_living_backend(&self, mut idx: usize) -> usize {
        idx = (idx + 1) % self.backends.len();
        loop {
            let backend = &self.backends[idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return idx;
            }
            idx = (idx + 1) % self.backends.len();
        }
    }

    async fn find_predecessor(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(failed_backend))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        srv_idx = (srv_idx - 1) % self.backends.len();
        loop {
            let backend = &self.backends[srv_idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return backend.clone();
            }
            srv_idx = (srv_idx - 1) % self.backends.len();
        }
    }

    async fn find_succesor(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(failed_backend))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        let next_srv = self.get_next_living_backend(srv_idx).await;
        return self.backends[next_srv].clone();
    }

    async fn get_keys(&self, srv: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            let keys_resp = client
                .keys(Pattern {
                    prefix: "".to_string(),
                    suffix: "".to_string(),
                })
                .await;
            if keys_resp.is_ok() {
                let keys = keys_resp.unwrap();
                return keys.into_inner().list;
            }
        }
        return Vec::new();
    }

    async fn get_list_keys(&self, srv: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            let keys_resp = client
                .list_keys(Pattern {
                    prefix: "".to_string(),
                    suffix: "".to_string(),
                })
                .await;
            if keys_resp.is_ok() {
                return keys_resp.unwrap().into_inner().list;
            }
        }
        return Vec::new();
    }

    async fn start_migration(&self, failed_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = format!("{}:{}", KEEPER_LOG, self.keeper_addr);
        let log_entry_val = format!("{}:{}:{}", FAIL_TASK, failed_backend, START_MSG);
        let log_srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(&log_entry_key))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        let log_srv_primary = &self.backends[log_srv_idx];

        let conn_primary = TribStorageClient::connect(log_srv_primary.clone()).await;
        if conn_primary.is_ok() {
            let mut client = conn_primary.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.clone(),
                value: log_entry_val.clone(),
            });
        }
    }

    async fn end_migration(&self, failed_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = format!("{}:{}", KEEPER_LOG, self.keeper_addr);
        let log_entry_val = format!("{}:{}:{}", FAIL_TASK, failed_backend, END_MSG);
        let log_srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(&log_entry_key))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        let log_srv_primary = &self.backends[log_srv_idx];

        let conn_primary = TribStorageClient::connect(log_srv_primary.clone()).await;
        if conn_primary.is_ok() {
            let mut client = conn_primary.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.clone(),
                value: log_entry_val.clone(),
            });
        }
    }

    async fn start_recover(&self, recover_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = format!("{}:{}", KEEPER_LOG, self.keeper_addr);
        let log_entry_val = format!("{}:{}:{}", REC_TASK, recover_backend.clone(), START_MSG);
        let log_srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(&log_entry_key))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        let log_srv_primary = &self.backends[log_srv_idx];

        let conn_primary = TribStorageClient::connect(log_srv_primary.clone()).await;
        if conn_primary.is_ok() {
            let mut client = conn_primary.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.clone(),
                value: log_entry_val.clone(),
            });
        }
    }

    async fn end_recover(&self, recover_backend: &String) {
        let mut log_srv_idx: usize;
        let log_entry_key = format!("{}:{}", KEEPER_LOG, self.keeper_addr);
        let log_entry_val = format!("{}:{}:{}", REC_TASK, recover_backend.clone(), END_MSG);
        let log_srv_idx = self
            .hashed_backends
            .binary_search(&cons_hash(&log_entry_key))
            .unwrap_or_else(|x| x % self.hashed_backends.len());
        let log_srv_primary = &self.backends[log_srv_idx];

        let conn_primary = TribStorageClient::connect(log_srv_primary.clone()).await;
        if conn_primary.is_ok() {
            let mut client = conn_primary.unwrap();
            client.list_append(KeyValue {
                key: log_entry_key.clone(),
                value: log_entry_val.clone(),
            });
        }
    }

    async fn get_key_string_value(&self, srv: &String, key: &String) -> Option<String> {
        let conn = TribStorageClient::connect(srv.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            let resp = client.get(Key { key: key.clone() }).await;
            if resp.is_ok() {
                return Some(resp.unwrap().into_inner().value.clone());
            }
        }
        return None;
    }

    async fn get_key_list_value(&self, srv: &String, key: &String) -> Vec<String> {
        let conn = TribStorageClient::connect(srv.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            let resp = client.list_get(Key { key: key.clone() }).await;
            if resp.is_ok() {
                return resp.unwrap().into_inner().list;
            }
        }
        return Vec::new();
    }

    async fn migrate_string_keyval(&self, key: &String, val: &String, to_backend: &String) {
        let conn = TribStorageClient::connect(to_backend.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            let _ = client
                .set(KeyValue {
                    key: key.clone(),
                    value: val.clone(),
                })
                .await;
        }
    }

    async fn migrate_list_keyval(&self, key: &String, vals: Vec<String>, to_backend: &String) {
        let conn = TribStorageClient::connect(to_backend.clone()).await;
        if conn.is_ok() {
            let mut client = conn.unwrap();
            for val in vals.iter() {
                let _ = client
                    .list_append(KeyValue {
                        key: key.clone(),
                        value: val.clone(),
                    })
                    .await;
            }
        }
    }

    // consider write log for starting migration for failed backend first
    // assuming no backend fails after a failure
    async fn migrate(&self, failed_backend: &String) {
        // use write-ahead log to tolerate keeper fault
        self.start_migration(failed_backend).await;

        let prev_srv = self.find_predecessor(failed_backend).await;
        let next_srv = self.find_succesor(failed_backend).await;

        // get the keys from prev_srv and next_srv
        let mut string_keys_prev_srv = Vec::new();
        let mut list_keys_prev_srv = Vec::new();

        string_keys_prev_srv.extend(self.get_keys(&prev_srv).await);
        list_keys_prev_srv.extend(self.get_list_keys(&prev_srv).await);
        let mut string_keys_succ_srv = Vec::new();
        let mut list_keys_succ_srv = Vec::new();
        string_keys_succ_srv.extend(self.get_keys(&next_srv).await);
        list_keys_succ_srv.extend(self.get_list_keys(&next_srv).await);

        let failed_idx = self
            .hashed_backends
            .binary_search(&cons_hash(failed_backend))
            .unwrap_or_else(|x| x % self.hashed_backends.len());

        // do successor migration first
        for string_key in string_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(string_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;
            let primary_srv = &self.backends[primary_idx];
            let is_failed_backend_primary =
                self.is_failed_backend_primary(insert_idx, failed_idx).await;

            if is_failed_backend_primary {
                // the succesor key now is the primary
                let next_succ_srv_idx =
                    self.get_next_replica_backend(primary_idx, string_key).await;
                let val = self.get_key_string_value(primary_srv, string_key).await;
                let next_succ_srv = &self.backends[next_succ_srv_idx];
                self.migrate_string_keyval(&string_key, &val.unwrap(), next_succ_srv)
                    .await;
            }
        }

        for list_key in list_keys_succ_srv.iter() {
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(list_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());

            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;
            let primary_srv = &self.backends[primary_idx];
            let is_failed_backend_primary =
                self.is_failed_backend_primary(insert_idx, failed_idx).await;

            if is_failed_backend_primary {
                // the succesor key now is the primary
                let next_succ_srv_idx = self.get_next_replica_backend(primary_idx, list_key).await;
                let val = self.get_key_list_value(primary_srv, list_key).await;
                let next_succ_srv = &self.backends[next_succ_srv_idx];
                self.migrate_list_keyval(list_key, val, next_succ_srv).await;
            }
        }

        // from the keys of previous server's,
        // identify whether it is primary or backup
        for string_key in string_keys_prev_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(string_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = &self.backends[primary_idx];
            if primary_srv.eq(&prev_srv) {
                // primary key at primary srv, make a copy
                let replica_idx = self.get_next_replica_backend(primary_idx, string_key).await;
                let replica_srv = &self.backends[replica_idx];
                let val = self.get_key_string_value(&primary_srv, string_key).await;
                self.migrate_string_keyval(string_key, &val.unwrap(), replica_srv)
                    .await;
            }
        }

        // loop string list and migrate
        for list_key in list_keys_prev_srv.iter() {
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(list_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;

            let primary_srv = &self.backends[primary_idx];
            if primary_srv.eq(&prev_srv) {
                // primary key at primary srv, make a copy
                let replica_idx = self.get_next_replica_backend(primary_idx, list_key).await;
                let replica_srv = &self.backends[replica_idx];
                let val = self.get_key_list_value(&primary_srv, list_key).await;
                self.migrate_list_keyval(list_key, val, replica_srv).await;
            }
        }

        self.end_migration(failed_backend).await;
    }

    async fn join(&self, recover_backend: &String) {
        // use write-ahead log to tolerate keeper fault
        self.start_recover(recover_backend).await;

        let prev_srv = self.find_predecessor(recover_backend).await;
        let next_srv = self.find_succesor(recover_backend).await;

        // get the keys from prev_srv and next_srv
        let mut string_keys_prev_srv = Vec::new();
        let mut list_keys_prev_srv = Vec::new();
        string_keys_prev_srv.extend(self.get_keys(&prev_srv).await);
        list_keys_prev_srv.extend(self.get_list_keys(&prev_srv).await);
        let mut string_keys_succ_srv = Vec::new();
        let mut list_keys_succ_srv = Vec::new();
        string_keys_succ_srv.extend(self.get_keys(&next_srv).await);
        list_keys_succ_srv.extend(self.get_list_keys(&next_srv).await);

        let recover_idx = self
            .hashed_backends
            .binary_search(&cons_hash(recover_backend))
            .unwrap_or_else(|x| x % self.hashed_backends.len());

        // do successor migration first
        for string_key in string_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(string_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;
            let primary_srv = &self.backends[primary_idx];
            if primary_idx == recover_idx {
                // data should be moved from succ srv to recover srv
                let val = self.get_key_string_value(primary_srv, string_key).await;
                self.migrate_string_keyval(string_key, &val.unwrap(), recover_backend)
                    .await;
            }
        }

        for list_key in list_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(list_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;
            let primary_srv = &self.backends[primary_idx];
            if primary_idx == recover_idx {
                // data should be moved from succ srv to recover srv
                let val = self.get_key_list_value(primary_srv, list_key).await;
                self.migrate_list_keyval(list_key, val, recover_backend)
                    .await;
            }
        }

        // from the keys of previous server's,
        // identify whether it is primary or backup
        for string_key in string_keys_prev_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(string_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = &self.backends[primary_idx];
            if primary_srv.eq(&prev_srv) {
                // recovered srv should store one copy
                let val = self.get_key_string_value(primary_srv, string_key).await;
                self.migrate_string_keyval(string_key, &val.unwrap(), recover_backend)
                    .await;
            }
        }

        // loop string list and migrate
        for list_key in list_keys_prev_srv.iter() {
            let insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(list_key))
                .unwrap_or_else(|x| x % self.hashed_backends.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;

            let primary_srv = &self.backends[primary_idx];
            if primary_srv.eq(&prev_srv) {
                // recovered srv should store one copy
                let val = self.get_key_list_value(primary_srv, list_key).await;
                self.migrate_list_keyval(list_key, val, recover_backend)
                    .await;
            }
        }

        self.end_recover(recover_backend).await;
    }
}
