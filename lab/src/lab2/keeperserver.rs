use super::keeperserverinit::{BackendStatus, ChordObject};
use super::utility::bin_aware_cons_hash;
use crate::keeper::rpc_keeper_server_client::RpcKeeperServerClient;
use crate::keeper::rpc_keeper_server_server::RpcKeeperServer;
use crate::keeper::Null;
use async_trait::async_trait;
use std::time::{Duration, SystemTime};
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{Key, KeyValue, Pattern};

pub struct KeeperServer {
    pub keeper_addr: String,
    pub hashed_keepers: Vec<u64>,
    pub keepers: Vec<String>,
    pub prev_keeper: String,

    // Begin of Ziheng's code
    pub list_back_recover: Vec<BackendStatus>, // Same as backends // list of backends that this keeper is responsible for (controls migrate and join)
    pub list_back_clock: Vec<String>, // list of backends that this keeper needs to sync the clock for. This list extends list_back_recover by 1 backend on left and right
    pub list_all_back_chord: Vec<ChordObject>, // list of all backends, no matter they are alive or not, no matter this keeper is responsible for or not
    pub prev_alive_keeper_list_back: Vec<BackendStatus>, // list of backends that previous keeper is responsible for
    // this keeper will check their liveness every 10 seconds
    pub debug_timer: SystemTime,
}

pub const MIGRATE_TASK_LOG: &str = "migrate_task_log";
pub const KEEPER_LOG: &str = "keeper_log";
pub const FAIL_TASK: &str = "fail";
pub const REC_TASK: &str = "recover";
pub const START_MSG: &str = "start";
pub const END_MSG: &str = "end";

#[async_trait]
impl RpcKeeperServer for KeeperServer {
    async fn dumb(
        &self,
        request: tonic::Request<Null>,
    ) -> Result<tonic::Response<Null>, tonic::Status> {
        return Ok(tonic::Response::new(Null {}));
    }
}

impl KeeperServer {
    // pub async fn recover_prev_keeper(&mut self, recovered_keeper: &String) {
    //     // redistribute backends
    //     let mut begin_pos = self.list_all_back_chord.binary_search(&ChordObject {
    //         hash: bin_aware_cons_hash(recovered_keeper),
    //         addr: recovered_keeper.clone()
    //     }).unwrap_or_else(|x| (x - 1 + self.list_all_back_chord.len()) % self.list_all_back_chord.len());
    //     begin_pos = (begin_pos + 1) % self.list_all_back_chord.len();
    //     let mut end_pos = self.list_all_back_chord.binary_search(&ChordObject {
    //         hash: bin_aware_cons_hash(&self.keeper_addr),
    //         addr: self.keeper_addr.clone()
    //     }).unwrap_or_else(|x| (x - 1 + self.list_all_back_chord.len()) % self.list_all_back_chord.len());
    //     end_pos = (end_pos + 1) % self.list_all_back_chord.len();

    //     while begin_pos != end_
    // }

    pub async fn check_prev_keeper(&mut self) {
        let conn = RpcKeeperServerClient::connect(self.prev_keeper.clone()).await;
        if !conn.is_ok() {
            // previous keeper is dead
            if true {
                println!(
                    "----Handle death of prev_keeper {}\t in keeper {}",
                    self.prev_keeper.clone(),
                    self.keeper_addr.clone()
                );
            }
            let pred_keeper = self.find_predecessor_keeper(&self.prev_keeper).await;
            // get the new backends
            let mut new_backends = Vec::new();
            let mut new_ext_backends = Vec::new();
            let mut insert_pos = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&pred_keeper),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| {
                    (x + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len()
                });

            new_ext_backends.push(self.list_all_back_chord[insert_pos].addr.clone());
            insert_pos = (1 + insert_pos) % self.list_all_back_chord.len();

            let mut end_pos = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&self.prev_keeper),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| {
                    (x + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len()
                });
            end_pos = (1 + end_pos) % self.list_all_back_chord.len();
            while insert_pos != end_pos {
                new_backends.push(self.list_all_back_chord[insert_pos].addr.clone());
                insert_pos = (1 + insert_pos) % self.list_all_back_chord.len();
            }
            new_ext_backends.extend(new_backends);
            new_ext_backends.push(self.list_all_back_chord[insert_pos].addr.clone());

            // check previous migration and join log
            let log_check_pass = self.check_prev_keeper_log().await;

            // check keys on one server is present on another one, if not
            if log_check_pass {
                let (string_keys_to_check, list_keys_to_check) =
                    self.get_backends_keys(new_ext_backends).await;
                self.check_string_keys_in_order(string_keys_to_check).await;
                self.check_list_keys_in_order(list_keys_to_check).await;
            }

            // upadte keeper states
            self.prev_keeper = pred_keeper.clone();
        }
    }

    async fn check_prev_keeper_log(&self) -> bool {
        // check migration log
        let log_entry_key = format!("{}:{}", KEEPER_LOG, self.prev_keeper);
        let log_entry_val = self.get_list_val(&log_entry_key).await;

        if !log_entry_val.is_empty() {
            let last_log = &log_entry_val[log_entry_val.len() - 1];
            let fields: Vec<&str> = last_log.as_str().split(":").collect();
            let is_fail_task = fields[0].to_string().eq(&FAIL_TASK.to_string());
            let failed_srv = fields[1].to_string();
            if last_log.ends_with(START_MSG) {
                // failed in the middle of a migration or join
                if is_fail_task {
                    self.migrate(&failed_srv).await;
                } else {
                    self.join(&failed_srv).await;
                }
                return false;
            }
        }
        return true;
    }

    async fn get_backends_keys(&self, new_ext_backends: Vec<String>) -> (Vec<String>, Vec<String>) {
        let mut string_keys = Vec::new();
        let mut list_keys = Vec::new();
        for backend in new_ext_backends {
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                string_keys.extend(self.get_keys(&backend).await);
                list_keys.extend(self.get_list_keys(&backend).await);
            }
        }
        return (string_keys, list_keys);
    }

    async fn check_list_keys_in_order(&self, keys: Vec<String>) {
        for key in keys {
            let mut idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x);
            let pidx = self.get_primary_backend(idx, &key).await;
            let primary_srv = &self.list_all_back_chord[pidx].addr;
            let mut insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            insert_idx =
                (insert_idx + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len();
            let should_primary_idx = self.get_next_living_backend(insert_idx).await;
            if should_primary_idx != pidx {
                // move
                let val = self.get_key_list_value(primary_srv, &key).await;
                self.migrate_list_keyval(
                    &key,
                    val,
                    &self.list_all_back_chord[should_primary_idx].addr,
                )
                .await;
            }
        }
    }

    async fn check_string_keys_in_order(&self, keys: Vec<String>) {
        for key in keys {
            let mut idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x);
            let pidx = self.get_primary_backend(idx, &key).await;
            let primary_srv = &self.list_all_back_chord[pidx].addr;
            let mut insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(&key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            insert_idx =
                (insert_idx + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len();
            let should_primary_idx = self.get_next_living_backend(insert_idx).await;
            if should_primary_idx != pidx {
                // move
                let val = self.get_key_string_value(primary_srv, &key).await;
                self.migrate_string_keyval(
                    &key,
                    &val.unwrap(),
                    &self.list_all_back_chord[should_primary_idx].addr,
                )
                .await;
            }
        }
    }

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
            let backend = &self.list_all_back_chord[idx].addr;
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
            idx = (idx + 1) % self.list_all_back_chord.len();
        }
    }

    async fn get_list_val(&self, key: &String) -> Vec<String> {
        let mut idx = self
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(key),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        for _ in 0..self.list_all_back_chord.len() {
            let backend = &self.list_all_back_chord[idx].addr;
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                let mut client = conn.unwrap();

                let list_key_resp = client.list_get(Key { key: key.clone() }).await;
                if list_key_resp.is_ok() {
                    return list_key_resp.unwrap().into_inner().list;
                }
            }
            idx = (idx + 1) % self.list_all_back_chord.len();
        }
        return Vec::new();
    }

    async fn is_failed_backend_primary(&self, mut insert_idx: usize, failed_idx: usize) -> bool {
        loop {
            if insert_idx == failed_idx {
                return true;
            }
            let backend = &self.list_all_back_chord[insert_idx].addr;
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return false;
            }
            insert_idx = (insert_idx + 1) % self.list_all_back_chord.len();
        }
    }

    async fn get_next_replica_backend(&self, mut idx: usize, key: &String) -> usize {
        idx = (idx + 1) % self.list_all_back_chord.len();
        loop {
            let backend = &self.list_all_back_chord[idx].addr;
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
            idx = (idx + 1) % self.list_all_back_chord.len();
        }
    }

    async fn get_next_living_backend(&self, mut idx: usize) -> usize {
        idx = (idx + 1) % self.list_all_back_chord.len();
        loop {
            let backend = &self.list_all_back_chord[idx].addr;
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return idx;
            }
            idx = (idx + 1) % self.list_all_back_chord.len();
        }
    }

    async fn find_predecessor_keeper(&self, failed_keeper: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_keepers
            .binary_search(&bin_aware_cons_hash(failed_keeper))
            .unwrap_or_else(|x| x % self.hashed_keepers.len());
        srv_idx = (srv_idx + self.keepers.len() - 1) % self.keepers.len();
        loop {
            let keeper = &self.keepers[srv_idx];
            let conn = TribStorageClient::connect(keeper.clone()).await;
            if conn.is_ok() {
                return keeper.clone();
            }
            srv_idx = (srv_idx + self.keepers.len() - 1) % self.keepers.len();
        }
    }

    async fn find_successor_keeper(&self, failed_keeper: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_keepers
            .binary_search(&bin_aware_cons_hash(failed_keeper))
            .unwrap_or_else(|x| x % self.hashed_keepers.len());
        srv_idx = (srv_idx + 1) % self.keepers.len();
        loop {
            let keeper = &self.keepers[srv_idx];
            let conn = TribStorageClient::connect(keeper.clone()).await;
            if conn.is_ok() {
                return keeper.clone();
            }
            srv_idx = (srv_idx + 1) % self.keepers.len();
        }
    }

    async fn find_predecessor_backend(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(failed_backend),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        srv_idx = (srv_idx + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len();
        loop {
            let backend = &self.list_all_back_chord[srv_idx].addr;
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                return backend.clone();
            }
            srv_idx =
                (srv_idx + self.list_all_back_chord.len() - 1) % self.list_all_back_chord.len();
        }
    }

    async fn find_succesor_backend(&self, failed_backend: &String) -> String {
        // binary search
        let mut srv_idx = self
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(failed_backend),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        let next_srv = self.get_next_living_backend(srv_idx).await;
        return self.list_all_back_chord[next_srv].addr.clone();
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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(&log_entry_key),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        let log_srv_primary = &self.list_all_back_chord[log_srv_idx].addr;

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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(&log_entry_key),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        let log_srv_primary = &self.list_all_back_chord[log_srv_idx].addr;

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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(&log_entry_key),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        let log_srv_primary = &self.list_all_back_chord[log_srv_idx].addr;

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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(&log_entry_key),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());
        let log_srv_primary = &self.list_all_back_chord[log_srv_idx].addr;

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
        log::info!("migrating {} to {}", key, to_backend);
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
    pub async fn migrate(&self, failed_backend: &String) {
        // use write-ahead log to tolerate keeper fault
        self.start_migration(failed_backend).await;

        let prev_srv = self.find_predecessor_backend(failed_backend).await;
        let next_srv = self.find_succesor_backend(failed_backend).await;

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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(failed_backend),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());

        // do successor migration first
        for string_key in string_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(string_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;
            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
            let is_failed_backend_primary =
                self.is_failed_backend_primary(insert_idx, failed_idx).await;

            if is_failed_backend_primary {
                // the succesor key now is the primary
                let next_succ_srv_idx =
                    self.get_next_replica_backend(primary_idx, string_key).await;
                let val = self.get_key_string_value(primary_srv, string_key).await;
                let next_succ_srv = &self.list_all_back_chord[next_succ_srv_idx].addr;
                self.migrate_string_keyval(&string_key, &val.unwrap(), next_succ_srv)
                    .await;
            }
        }

        for list_key in list_keys_succ_srv.iter() {
            let insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(list_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());

            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;
            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
            let is_failed_backend_primary =
                self.is_failed_backend_primary(insert_idx, failed_idx).await;

            if is_failed_backend_primary {
                // the succesor key now is the primary
                let next_succ_srv_idx = self.get_next_replica_backend(primary_idx, list_key).await;
                let val = self.get_key_list_value(primary_srv, list_key).await;
                let next_succ_srv = &self.list_all_back_chord[next_succ_srv_idx].addr;
                self.migrate_list_keyval(list_key, val, next_succ_srv).await;
            }
        }

        // from the keys of previous server's,
        // identify whether it is primary or backup
        for string_key in string_keys_prev_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(string_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
            if primary_srv.eq(&prev_srv) {
                // primary key at primary srv, make a copy
                let replica_idx = self.get_next_replica_backend(primary_idx, string_key).await;
                let replica_srv = &self.list_all_back_chord[replica_idx].addr;
                let val = self.get_key_string_value(&primary_srv, string_key).await;
                self.migrate_string_keyval(string_key, &val.unwrap(), replica_srv)
                    .await;
            }
        }

        // loop string list and migrate
        for list_key in list_keys_prev_srv.iter() {
            let insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(list_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;

            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
            if primary_srv.eq(&prev_srv) {
                // primary key at primary srv, make a copy
                let replica_idx = self.get_next_replica_backend(primary_idx, list_key).await;
                let replica_srv = &self.list_all_back_chord[replica_idx].addr;
                let val = self.get_key_list_value(&primary_srv, list_key).await;
                self.migrate_list_keyval(list_key, val, replica_srv).await;
            }
        }

        self.end_migration(failed_backend).await;
    }

    pub async fn join(&self, recover_backend: &String) {
        // use write-ahead log to tolerate keeper fault
        self.start_recover(recover_backend).await;

        let prev_srv = self.find_predecessor_backend(recover_backend).await;
        let next_srv = self.find_succesor_backend(recover_backend).await;

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
            .list_all_back_chord
            .binary_search(&ChordObject {
                hash: bin_aware_cons_hash(recover_backend),
                addr: "".to_string(),
            })
            .unwrap_or_else(|x| x % self.list_all_back_chord.len());

        // do successor migration first
        for string_key in string_keys_succ_srv.iter() {
            // binary search the key hash
            let insert_idx = self
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(string_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;
            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
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
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(list_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;
            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
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
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(string_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;

            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
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
                .list_all_back_chord
                .binary_search(&ChordObject {
                    hash: bin_aware_cons_hash(list_key),
                    addr: "".to_string(),
                })
                .unwrap_or_else(|x| x % self.list_all_back_chord.len());
            let primary_idx = self.get_primary_backend(insert_idx, list_key).await;

            let primary_srv = &self.list_all_back_chord[primary_idx].addr;
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
