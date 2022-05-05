use super::utility::cons_hash;
use crate::keeper::rpc_keeper_server_client::RpcKeeperServerClient;
use crate::keeper::rpc_keeper_server_server::RpcKeeperServer;
use crate::keeper::Null;
use async_trait::async_trait;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{Key, KeyValue, Pattern};

// Begin of ziheng's import and const
use crate::keeper::rpc_keeper_server_server::RpcKeeperServerServer;
use std::cmp::Ordering;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::Sender;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot::Receiver;
use tonic::transport::Error;
use tonic::transport::Server;
use tribbler::err::TribResult;
use tribbler::rpc;

const PRINT_DEBUG: bool = true;
const CLOCK_TIMEOUT_MS: u64 = 300; // when a backend failed to connect, we sleep for CLOCK_TIMEOUT_MS ms, and give it one chance
pub const KEEPER_INIT_DELAY_MS: u64 = 500; // when a keeper is initialized, sleep for KEEPER_INIT_DELAY_MS ms, and then start the heart_beat loop
                                           // End of ziheng's import and const

pub struct KeeperServer {
    pub keeper_addr: String,
    pub backends: Vec<String>,
    pub hashed_backends: Vec<u64>,
    pub hashed_keepers: Vec<u64>,
    pub keepers: Vec<String>,
    pub prev_keeper: String,

    // Begin of Ziheng's code

    // TODO: check with Qizeng whether backends are backends this keeper is responsible for (controls migrate and join), but keepers are all keepers
    // TODO: check with Qizeng whether backends.len() is 0 will matter
    pub list_back_recover: Vec<BackendStatus>, // Same as backends // list of backends that this keeper is responsible for (controls migrate and join)
    pub list_back_clock: Vec<String>, // list of backends that this keeper needs to sync the clock for. This list extends list_back_recover by 1 backend on left and right
    pub list_all_back_chord: Vec<ChordObject>, // list of all backends, no matter they are alive or not, no matter this keeper is responsible for or not
    pub prev_alive_keeper_list_back: Vec<BackendStatus>, // list of backends that previous keeper is responsible for
                                                         // this keeper will check their liveness every 10 seconds
}

impl KeeperServer {
    // given all backends' addr and keepers' addr, use hash to init the following fields:
    //   backends, hashed_backends, keepers, hashed_keepers, list_back_recover, list_back_clock, list_all_back_chord
    pub fn init_back_recover_and_clock(
        &mut self,
        list_all_back: Vec<String>,
        list_all_keep: Vec<String>,
    ) {
        // put all backs to a list as ChordObject, sort this list
        let mut list_all_back_hash: Vec<u64> = Vec::new();

        for back in list_all_back {
            let o = ChordObject {
                hash: cons_hash(&back.clone()), // hash = hash(addr)
                addr: back,
            };
            self.list_all_back_chord.push(o);
        }

        self.list_all_back_chord.sort();

        for back_obj in self.list_all_back_chord.clone() {
            list_all_back_hash.push(back_obj.hash);
        }

        // put all backs to a list as ChordObject, sort this list
        let mut list_all_keeper_chord: Vec<ChordObject> = Vec::new();

        for keeper in list_all_keep {
            let o = ChordObject {
                hash: cons_hash(&keeper.clone()), // hash = hash(addr)
                addr: keeper,
            };
            list_all_keeper_chord.push(o);
        }

        list_all_keeper_chord.sort();

        let (mut index_self, mut i) = (0, 0);
        for keeper_obj in list_all_keeper_chord.clone() {
            self.keepers.push(keeper_obj.addr.clone());
            self.hashed_keepers.push(keeper_obj.hash.clone());
            if keeper_obj.addr.clone() == self.keeper_addr {
                index_self = i;
            }
            i += 1;
        }

        //figure out what backends our keeper needs to take care of, and init fields in KeeperServer
        let (first_back_index, last_back_index) = (0, 0);
        for back_obj in self.list_all_back_chord.clone() {
            // compute the index of the keeper that should take care of this backend
            let mut index_keeper = self
                .hashed_keepers
                .binary_search(&back_obj.hash)
                .unwrap_or_else(|x| x % self.hashed_keepers.len());
            if index_keeper > self.hashed_keepers.len() - 1 {
                index_keeper = 0; // when index_keeper is larger than the index of the last keeper, this backend belongs to the first keeper
            }

            if index_keeper == index_self {
                self.backends.push(back_obj.addr.clone());
                self.hashed_backends.push(back_obj.hash.clone());
                self.add_back_recover(back_obj.addr.clone());
            }
        }

        self.update_back_clock();
    }

    fn add_back_recover(&mut self, back: String) {
        self.list_back_recover.push(BackendStatus {
            back_addr: back,
            liveness: true, // TODO: check if this is true or false
        });
    }

    // this function inits list_back_clock
    // it can also be used anytime to update list_back_clock as long as field backends is up-to-date
    fn update_back_clock(&mut self) {
        self.list_back_clock = Vec::new();
        if self.backends.len() == 0 {
            return;
        }

        let mut list_all_back_hash: Vec<u64> = Vec::new();
        for back_obj in self.list_all_back_chord.clone() {
            list_all_back_hash.push(back_obj.hash);
        }

        // compute the list of index of backends self is responsible for
        let mut list_recover_back_index: Vec<usize> = Vec::new();
        for back_hash in self.hashed_backends.clone() {
            let index_back = list_all_back_hash
                .binary_search(&back_hash)
                .unwrap_or_else(|x| x % list_all_back_hash.len());
            list_recover_back_index.push(index_back);
        }
        list_recover_back_index.sort();

        // compute the two index by extending the list of index to left and right by 1
        // Example: responsible for [3,4,5,6], all backends [0,1,2,3,4,5,6,7,8] will have 2 and 7
        //                          [5,6,7,8], all backends [0,..,8] will have 4 and 0
        //                          [0,1,2,8], all backends [0,..,8] will have 7 and 3
        let (mut l, mut r) = find_lr(list_recover_back_index.clone());
        let len = list_all_back_hash.len();
        l = (l - 1 + len) % len;
        r = (r + 1) % len;

        // if append l and r to list, sort list, and remove duplicate
        list_recover_back_index.push(l);
        list_recover_back_index.push(r);
        list_recover_back_index.sort();
        list_recover_back_index.dedup();

        // init list_back_clock
        let list_all_back_chord_copy = self.list_all_back_chord.clone();
        for index in list_recover_back_index {
            self.list_back_clock
                .push(list_all_back_chord_copy[index].addr.clone());
        }
    }

    pub fn init_prev_keeper(&mut self) {
        let keepers_len = self.keepers.len();
        let index_self = self
            .keepers
            .binary_search(&self.keeper_addr.clone())
            .unwrap_or_else(|x| x % keepers_len);
        let index_prev = (index_self - 1 + keepers_len) % keepers_len;
        self.prev_keeper = self.keepers.clone()[index_prev].clone();
    }

    // when any keeper between self and self.prev_keeper becomes alive, update backends, hashed_backends, list_back_recover, list_back_clock, prev_keeper, prev_alive_keeper_list_back
    pub async fn check_new_keeper_between(&mut self) {
        let mut list_keeper_between: Vec<String> = Vec::new();
        let (mut index_prev, mut index_self) = (0, 0);
        let mut i = 0;
        for keeper in self.keepers.clone() {
            if keeper == self.keeper_addr {
                index_self = i;
            }
            if keeper == self.prev_keeper {
                index_prev = i;
            }
            i += 1;
        }
        // case 1: index_self == index_prev. Then all other keepers should be in list
        // case 2: index_self > index_prev. Then keepers larger than index_prev and smaller than index_self should be in list
        // case 3: index_self < index_prev. Then keepers smaller than index_self or larger than index_prev should be in list
        let list_all_keeper = self.keepers.clone();
        if index_self == index_prev {
            for j in 0..self.keepers.len() {
                if j != index_self {
                    list_keeper_between.push(list_all_keeper[j].clone());
                }
            }
        } else if index_self > index_prev {
            for j in 0..self.keepers.len() {
                if index_prev < j && j < index_self {
                    list_keeper_between.push(list_all_keeper[j].clone());
                }
            }
        } else {
            for j in 0..self.keepers.len() {
                if index_prev < j || j < index_self {
                    list_keeper_between.push(list_all_keeper[j].clone());
                }
            }
        }

        if list_keeper_between.len() == 0 {
            return;
        }

        // check whether one keeper between is alive
        // Note: all keepers between should be dead in the previous heartbeat, and there is at most one keeper between can be alive now, because of lab's assumption
        let mut added_keeper: Option<String> = None;
        for keeper in list_keeper_between {
            let conn = RpcKeeperServerClient::connect(keeper.clone()).await;
            if conn.is_ok() {
                added_keeper = Some(keeper);
                break; // at most one added keeper
            }
        }
        match added_keeper {
            Some(added_keeper) => {
                // We found an added keeper. Need to update self's fields so that backends that should be handled by
                let mut list_remove_back: Vec<String> = Vec::new();

                // find all backends that added_keeper is responsible for, using list_triple_hash, which
                //      is a sorted and no-duplicate vector of keeper's hash: [self.hash(), added_keeper.hash(), prev_keeper.hash()]
                //      and length is 2 or 3 (doesn't matter)
                let mut list_triple_chord: Vec<ChordObject> = vec![
                    ChordObject {
                        addr: self.keeper_addr.clone(),
                        hash: cons_hash(&self.keeper_addr.clone()),
                    },
                    ChordObject {
                        addr: added_keeper.clone(),
                        hash: cons_hash(&added_keeper.clone()),
                    },
                    ChordObject {
                        addr: self.prev_keeper.clone(),
                        hash: cons_hash(&self.prev_keeper.clone()),
                    },
                ];
                list_triple_chord.sort();
                list_triple_chord.dedup();
                let mut list_triple_hash: Vec<u64> = Vec::new();
                for obj in list_triple_chord.clone() {
                    list_triple_hash.push(obj.hash.clone());
                }
                for back_obj in self.list_all_back_chord.clone() {
                    let mut index_keeper = list_triple_hash
                        .binary_search(&back_obj.hash)
                        .unwrap_or_else(|x| x % list_triple_hash.len());
                    if index_keeper > list_triple_hash.len() - 1 {
                        index_keeper = 0; // when index_keeper is larger than the index of the last keeper, this backend belongs to the first keeper
                    }

                    if list_triple_chord.clone()[index_keeper].addr == added_keeper {
                        list_remove_back.push(back_obj.addr.clone());
                    }
                }

                // update self.backends, hashed_backends, list_back_recover, list_back_clock, prev_keeper, prev_alive_keeper_list_back
                let mut new_backends: Vec<String> = Vec::new();
                for back in self.backends.clone() {
                    match list_remove_back
                        .clone()
                        .into_iter()
                        .find(|x| *x == back.clone())
                    {
                        Some(_) => (),
                        None => {
                            // didn't find back in list_remove_back, meaning this back should be in new self.backends
                            new_backends.push(back.clone());
                        }
                    }
                }
                self.backends = new_backends.clone();

                self.deduct_back_recover(new_backends.clone());
                self.prev_keeper = added_keeper.clone();
                // TODO: do we still need prev_alive_keeper_list_back?

                self.hashed_backends = Vec::new();
                for back in new_backends.clone() {
                    self.hashed_backends.push(cons_hash(&back.clone()));
                }
                self.update_back_clock();
            }
            None => return,
        }
    }

    fn find_back_recover(&self, addr: String) -> Option<BackendStatus> {
        for back in self.list_back_recover.clone() {
            if back.back_addr == addr {
                return Some(back);
            }
        }
        return None;
    }

    fn deduct_back_recover(&mut self, list_remove: Vec<String>) {
        let mut new_list_back_recover: Vec<BackendStatus> = Vec::new();
        for back in self.list_back_recover.clone() {
            match list_remove
                .clone()
                .into_iter()
                .find(|x| *x == back.back_addr.clone())
            {
                Some(_) => (),
                None => {
                    // didn't find back in list_remove, so it should be in new_list
                    new_list_back_recover.push(BackendStatus {
                        back_addr: back.back_addr.clone(),
                        liveness: back.liveness,
                    });
                }
            }
        }
        self.list_back_recover = new_list_back_recover;
    }

    fn set_back_recover_liveness(&mut self, addr: String, liveness: bool) {
        let mut list_back_copy = self.list_back_recover.clone();
        for mut back in list_back_copy.iter_mut() {
            if back.back_addr == addr {
                back.liveness = liveness;
            }
        }
        self.list_back_recover = list_back_copy;
    }

    // hear_beat is called every second. It will:
    // (0) sync the clock of all backends in list_back_clock
    // (1) if fail to connect to a backend, and it is in list_back_recover, and it was alive in the last round, then we wait for CLOCK_TIMEOUT_MS milliseconds and try again
    //      if it still fails, we observed a backend failure. We will run the migration procedure
    // (2) if suc to connect to a backend, and it is in list_back_recover, and it was dead in the last round
    //      we observed a backend join. We will run the join procedure
    // (3) check if its previous alive keeper is now dead
    // (4) check if any keeper between it and its previous alive keeper is now added
    //      if so, update prev_keeper, and all fields related to backends
    pub async fn heart_beat(&mut self) -> TribResult<()> {
        let now = SystemTime::now();

        let mut max_timestamp = 0;
        let mut list_dead_addr: Vec<String> = Vec::new();
        let mut list_alive_addr: Vec<String> = Vec::new();
        for addr in self.list_back_clock.clone() {
            // TODO: see how to reuse connection here and below
            match TribStorageClient::connect(addr.clone()).await {
                Ok(mut client) => {
                    let r = client
                        .clock(rpc::Clock {
                            timestamp: max_timestamp,
                        })
                        .await?;
                    let t = r.into_inner().timestamp;
                    if t > max_timestamp {
                        max_timestamp = t;
                    }
                    list_alive_addr.push(addr.clone());
                }
                Err(e) => {
                    list_dead_addr.push(addr.clone());
                }
            };
        }

        if PRINT_DEBUG {
            println!(
                "This keeper observe {} alive backends and {} dead backends",
                list_alive_addr.len(),
                list_dead_addr.len()
            );
        }

        // sync the clock of alive backends
        for addr in list_alive_addr.clone() {
            let mut client = TribStorageClient::connect(addr.clone()).await?;
            let r = client
                .clock(rpc::Clock {
                    timestamp: max_timestamp,
                })
                .await?;
        }

        // (2) if suc to connect to a backend, and it is in list_back_recover, and it was dead in the last round
        //      we observed a backend join. We will run the join procedure
        for addr in list_alive_addr {
            match self.find_back_recover(addr.clone()) {
                Some(back) => {
                    if back.liveness == false {
                        self.set_back_recover_liveness(back.back_addr, true);

                        // TODO: handle join
                    }
                }
                None => println!("Error: impossible None result in step (2)"),
            }
        }

        // (3) check if its previous alive keeper is now dead
        // TODO: ask Qizeng if this function also handles the update of self.backends, and migration of dead previous keeper's backends
        if self.keeper_addr != self.prev_keeper {
            self.check_prev_keeper();
        }

        // (4) check if any keeper between it and its previous alive keeper is now added
        //      if so, update prev_keeper, and all fields related to backends
        self.check_new_keeper_between();

        // sleep before (1)
        // TODO: do we really need this?
        sleep(Duration::from_millis(CLOCK_TIMEOUT_MS));

        // (1) if fail to connect to a backend, and it is in list_back_recover, and it was alive in the last round, then we wait for CLOCK_TIMEOUT_MS milliseconds and try again
        //      if it still fails, we observed a backend failure. We will run the migration procedure

        for addr in list_dead_addr {
            match self.find_back_recover(addr.clone()) {
                Some(back) => {
                    if back.liveness == true {
                        let addr = back.back_addr;
                        match TribStorageClient::connect(addr.clone()).await {
                            Ok(mut client) => {
                                // this backend is actually alive. Sync its clock
                                let r = client
                                    .clock(rpc::Clock {
                                        timestamp: max_timestamp,
                                    })
                                    .await?;
                            }
                            Err(_) => {
                                // this backend is really newly dead
                                self.set_back_recover_liveness(addr.clone(), false);

                                // TODO: handle migration
                            }
                        };
                    }
                }
                None => println!("Error: impossible None result in step (1)"),
            }
        }

        if PRINT_DEBUG {
            match now.elapsed() {
                Ok(elapsed) => {
                    println!("{}", elapsed.as_millis());
                }
                Err(e) => {
                    println!("Error in heartbeat() : now.elapsed has an error: {:?}", e);
                }
            }
        }
        return Ok(());
    }
}

// given a vector of consequentive integers on a chord ring (which is, allow at most 1 break in vector) like [2,3,4,5,6], or [0,1,2,3,9]
// find_lr find the (left, right) element of the vector, like (2,6), or (9,3)
fn find_lr(v: Vec<usize>) -> (usize, usize) {
    let mut l = 0;
    let mut r = 0;
    let mut i = 0;
    while (i < v.len()) {
        if (v[i] - v[r] <= 1) {
            r = i;
            if (r == v.len()) {
                break;
            } else {
                i += 1;

                continue;
            }
        } else {
            l = i;
            break;
        }
    }
    return (l, r);
}

// BackendStatus is a struct that Keeper stores, recording each backend's addr and liveness
#[derive(Debug, Clone)]
pub struct BackendStatus {
    back_addr: String, // addr of the backend
    liveness: bool,    // whether this backend is alive or not
}

// ChordObject is an object stored on the ring of Chord
#[derive(Debug, Clone)]
pub struct ChordObject {
    hash: u64, // hash = hash(addr)
    addr: String,
}

impl Ord for ChordObject {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialOrd for ChordObject {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ChordObject {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for ChordObject {}

// TODO: ask Qizeng about my trait
pub struct MyKeeperRpcServer {
    addr: SocketAddr,
}

#[async_trait]
impl RpcKeeperServer for MyKeeperRpcServer {
    async fn dumb(
        &self,
        request: tonic::Request<Null>,
    ) -> Result<tonic::Response<Null>, tonic::Status> {
        return Ok(tonic::Response::new(Null {}));
    }
}

pub async fn serve_my_keeper_rpc(
    addr: SocketAddr,
    ready: Option<Sender<bool>>,
    mut shutdown: Receiver<()>,
) -> Result<(), Error> {
    match ready {
        Some(send) => {
            let send_result = send.send(true);
            match send_result {
                Ok(()) => (),
                Err(e) => (),
            }
        }
        None => (),
    }

    let server = MyKeeperRpcServer { addr: addr };

    Server::builder()
        .add_service(RpcKeeperServerServer::new(server))
        .serve_with_shutdown(addr, block_fn(shutdown))
        .await?;

    return Ok(());
}

pub async fn block_fn(mut recv: Receiver<()>) {
    tokio::select! {
        _ = &mut recv => {
            // exist if shutdown has received
            return ();
        }
    };
}

// End of Ziheng's code

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
    async fn check_prev_keeper(&mut self) {
        let conn = RpcKeeperServerClient::connect(self.prev_keeper.clone()).await;
        if !conn.is_ok() {
            // previous keeper is dead
            let pred_keeper = self.find_predecessor_keeper(&self.prev_keeper).await;
            // get the new backends
            let mut new_backends = Vec::new();
            let mut new_ext_backends = Vec::new();
            let mut insert_pos = self
                .hashed_backends
                .binary_search(&cons_hash(&pred_keeper))
                .unwrap_or_else(|x| (x - 1) % self.hashed_backends.len());
            new_ext_backends.push(self.backends[insert_pos].clone());
            insert_pos = (1 + insert_pos) % self.hashed_backends.len();

            let mut end_pos = self
                .hashed_backends
                .binary_search(&cons_hash(&self.prev_keeper))
                .unwrap_or_else(|x| (x - 1) % self.hashed_backends.len());
            end_pos = (1 + end_pos) % self.hashed_backends.len();
            while insert_pos != end_pos {
                new_backends.push(self.backends[insert_pos].clone());
                insert_pos = (1 + insert_pos) % self.hashed_backends.len();
            }
            new_ext_backends.extend(new_backends);
            new_ext_backends.push(self.backends[insert_pos].clone());

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
            // self.backends.extend(&new_backends);
            // todo: update other fields as well
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
                .hashed_backends
                .binary_search(&cons_hash(&key))
                .unwrap_or_else(|x| x);
            let pidx = self.get_primary_backend(idx, &key).await;
            let primary_srv = &self.backends[pidx];
            let mut insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(&key))
                .unwrap_or_else(|x| x);
            insert_idx = (insert_idx - 1) % self.backends.len();
            let should_primary_idx = self.get_next_living_backend(insert_idx).await;
            if should_primary_idx != pidx {
                // move
                let val = self.get_key_list_value(primary_srv, &key).await;
                self.migrate_list_keyval(&key, val, &self.backends[should_primary_idx])
                    .await;
            }
        }
    }

    async fn check_string_keys_in_order(&self, keys: Vec<String>) {
        for key in keys {
            let mut idx = self
                .hashed_backends
                .binary_search(&cons_hash(&key))
                .unwrap_or_else(|x| x);
            let pidx = self.get_primary_backend(idx, &key).await;
            let primary_srv = &self.backends[pidx];
            let mut insert_idx = self
                .hashed_backends
                .binary_search(&cons_hash(&key))
                .unwrap_or_else(|x| x);
            insert_idx = (insert_idx - 1) % self.backends.len();
            let should_primary_idx = self.get_next_living_backend(insert_idx).await;
            if should_primary_idx != pidx {
                // move
                let val = self.get_key_string_value(primary_srv, &key).await;
                self.migrate_string_keyval(&key, &val.unwrap(), &self.backends[should_primary_idx])
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

    async fn get_list_val(&self, key: &String) -> Vec<String> {
        let mut idx = self
            .hashed_backends
            .binary_search(&cons_hash(key))
            .unwrap_or_else(|x| x);
        for _ in 0..self.backends.len() {
            let backend = &self.backends[idx];
            let conn = TribStorageClient::connect(backend.clone()).await;
            if conn.is_ok() {
                let mut client = conn.unwrap();

                let list_key_resp = client.list_get(Key { key: key.clone() }).await;
                if list_key_resp.is_ok() {
                    return list_key_resp.unwrap().into_inner().list;
                }
            }
            idx = (idx + 1) % self.backends.len();
        }
        return Vec::new();
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

    async fn find_predecessor_keeper(&self, failed_keeper: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_keepers
            .binary_search(&cons_hash(failed_keeper))
            .unwrap_or_else(|x| x % self.hashed_keepers.len());
        srv_idx = (srv_idx - 1) % self.keepers.len();
        loop {
            let keeper = &self.keepers[srv_idx];
            let conn = TribStorageClient::connect(keeper.clone()).await;
            if conn.is_ok() {
                return keeper.clone();
            }
            srv_idx = (srv_idx - 1) % self.keepers.len();
        }
    }

    async fn find_successor_keeper(&self, failed_keeper: &String) -> String {
        // binary search
        let mut srv_idx = self
            .hashed_keepers
            .binary_search(&cons_hash(failed_keeper))
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

    async fn find_succesor_backend(&self, failed_backend: &String) -> String {
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
