 
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{Pattern};

pub struct KeeperServer {
    pub backends: Vec<String>,
    pub hashed_backends: Vec<i32>,
    pub assigned: Vec<String>,
    pub liveliness: Vec<bool>,
}

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

    // consider write log for starting migration for failed backend first
    async fn migrate(&self, failed_backend: &String) -> Result<()> {

        let prev_srv = self.find_predecessor(failed_backend);
        let next_srv = self.find_succesor(failed_backend);

        // get the keys from prev_srv and next_srv
        let mut string_keys = Vec::new();
        let mut list_keys = Vec::new();
        string_keys.extend(self.get_keys(&prev_srv));
        list_keys.extend(self.)

        // todo: consider making the following operations atomic
        // loop string keys and migrate 
        for &string_key in string_keys.iter() {
            // binary search the key hash
            let insert_idx = self.hashed_backends.binary_search(string_key).unwrap_or_else(|x| x);
            let insert_backend = self.hashed_backends[insert_idx];
            let primary_idx = self.get_primary_backend(insert_idx, string_key).await;
            let replica_idx = self.get_next_replica_backend(primary_idx, string_key).await;
            let available_idx = self.get_next_living_backend(replica_idx).await;

            // copy the key to 
        }

        // loop string list and migrate
        for &list_key in list_keys.0.iter() {

        }
    }


    async fn fill(recover_backend: &String) -> Result<()> {

    }
}