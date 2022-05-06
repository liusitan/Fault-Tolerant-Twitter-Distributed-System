use super::keeperserver::KeeperServer;
use super::utility::bin_aware_cons_hash;
use crate::keeper::rpc_keeper_server_client::RpcKeeperServerClient;
use crate::keeper::rpc_keeper_server_server::RpcKeeperServer;
use crate::keeper::rpc_keeper_server_server::RpcKeeperServerServer;
use crate::keeper::Null;
use async_trait::async_trait;
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
use tribbler::rpc::trib_storage_client::TribStorageClient;

pub const PRINT_DEBUG: i32 = 1; // 0 not print, 1 print only status, 2 print status and less, 3 print all
pub const PRINT_STATUS: bool = true;
const CLOCK_TIMEOUT_MS: u64 = 300; // when a backend failed to connect, we sleep for CLOCK_TIMEOUT_MS ms, and give it one chance
pub const KEEPER_INIT_DELAY_MS: u64 = 500; // when a keeper is initialized, sleep for KEEPER_INIT_DELAY_MS ms, and then start the heart_beat loop
                                           // End of ziheng's import and const

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

// ChordObject is an object stored on the ring of Chord
#[derive(Debug, Clone)]
pub struct ChordObject {
    pub hash: u64, // hash = hash(addr)
    pub addr: String,
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

// BackendStatus is a struct that Keeper stores, recording each backend's addr and liveness
#[derive(Debug, Clone)]
pub struct BackendStatus {
    pub back_addr: String, // addr of the backend
    pub liveness: bool,    // whether this backend is alive or not
}

impl Ord for BackendStatus {
    fn cmp(&self, other: &Self) -> Ordering {
        bin_aware_cons_hash(&self.back_addr.clone())
            .cmp(&bin_aware_cons_hash(&other.back_addr.clone()))
    }
}

impl PartialOrd for BackendStatus {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BackendStatus {
    fn eq(&self, other: &Self) -> bool {
        self.back_addr.clone() == other.back_addr.clone()
    }
}

impl Eq for BackendStatus {}

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
                hash: bin_aware_cons_hash(&back.clone()), // hash = hash(addr)
                addr: back,
            };
            self.list_all_back_chord.push(o);
        }

        self.list_all_back_chord.sort();

        for back_obj in self.list_all_back_chord.clone() {
            list_all_back_hash.push(back_obj.hash);
        }

        if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!("===Print list_all_back_chord");
            for back_obj in self.list_all_back_chord.clone() {
                println!(
                    "Back_addr: {}, hash: {}",
                    back_obj.addr.clone(),
                    back_obj.hash
                );
            }
            println!("===End of list_all_back_chord");
        }

        // put all keepers to a list as ChordObject, sort this list
        let mut list_all_keeper_chord: Vec<ChordObject> = Vec::new();

        for keeper in list_all_keep {
            let o = ChordObject {
                hash: bin_aware_cons_hash(&keeper.clone()), // hash = hash(addr)
                addr: keeper,
            };
            list_all_keeper_chord.push(o);
        }

        list_all_keeper_chord.sort();

        let (mut index_self, mut i) = (0, 0);
        for keeper_obj in list_all_keeper_chord.clone() {
            self.keepers.push(keeper_obj.addr.clone());
            self.hashed_keepers.push(keeper_obj.hash.clone());
            if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
                println!(
                    "Before eq: first {}\tsecond {}",
                    keeper_obj.addr.clone(),
                    self.keeper_addr
                );
            }
            if keeper_obj.addr.clone() == self.keeper_addr {
                if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
                    println!("Keeper_addr {} is at index {}", keeper_obj.addr.clone(), i);
                }
                index_self = i;
            }
            i += 1;
        }

        if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!("===Print list_all_keeper_chord");
            for k_obj in list_all_keeper_chord.clone() {
                println!("Keeper_addr: {}, hash: {}", k_obj.addr.clone(), k_obj.hash);
            }
            println!("===End of list_all_keeper_chord");
        }

        if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!("===Print back to keeper");
        }

        //figure out what backends our keeper needs to take care of, and init fields in KeeperServer
        for back_obj in self.list_all_back_chord.clone() {
            // compute the index of the keeper that should take care of this backend
            let mut index_keeper = self
                .hashed_keepers
                .binary_search(&back_obj.hash)
                .unwrap_or_else(|x| x % self.hashed_keepers.len());
            if index_keeper > self.hashed_keepers.len() - 1 {
                index_keeper = 0; // when index_keeper is larger than the index of the last keeper, this backend belongs to the first keeper
            }

            if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
                println!(
                    "Back_addr {}\tbelongs to keeper {}",
                    back_obj.addr.clone(),
                    self.keepers.clone()[index_keeper]
                );
            }

            if index_keeper == index_self {
                if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
                    println!(
                        "Confirm that back_addr {} is taken by {}",
                        back_obj.addr.clone(),
                        DEBUG_KEEPER.to_string()
                    );
                }
                self.add_back_recover(back_obj.addr.clone());
            }
        }

        if PRINT_DEBUG == 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!("===End back to keeper");
        }

        self.update_back_clock();
    }

    fn add_back_recover(&mut self, back: String) {
        self.list_back_recover.push(BackendStatus {
            back_addr: back,
            liveness: true, // TODO: check if this is true or false
        });
        self.list_back_recover.sort();
        if self.list_back_recover.len() >= 2 {
            let copy = self.list_back_recover.clone();
            assert_eq!(
                bin_aware_cons_hash(&copy[0].back_addr.clone())
                    < bin_aware_cons_hash(&copy[1].back_addr.clone()),
                true
            );
        }
    }

    // this function inits list_back_clock
    // it can also be used anytime to update list_back_clock as long as field backends is up-to-date
    fn update_back_clock(&mut self) {
        if PRINT_DEBUG >= 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!(
                "====Before update_back_clock in keeper {}, we have list_back_clock of size {}:",
                self.keeper_addr,
                self.list_back_clock.len()
            );
            for back in self.list_back_clock.clone() {
                println!("{}", back);
            }
            println!("====End of list_back_clock\n");

            println!(
                "====We have list_back_recover of size {}:",
                self.list_back_recover.len()
            );
            for back in self.list_back_recover.clone() {
                println!("{}", back.back_addr);
            }
            println!("====End of list_back_recover\n");
        }
        self.list_back_clock = Vec::new();
        if self.list_back_recover.len() == 0 {
            return;
        }

        let mut list_all_back_hash: Vec<u64> = Vec::new();
        for back_obj in self.list_all_back_chord.clone() {
            list_all_back_hash.push(back_obj.hash);
        }

        // compute the list of index of backends self is responsible for
        let mut list_recover_back_index: Vec<usize> = Vec::new();
        for back_obj in self.list_back_recover.clone() {
            let index_back = list_all_back_hash
                .binary_search(&bin_aware_cons_hash(&back_obj.back_addr.clone()))
                .unwrap_or_else(|x| x % list_all_back_hash.len());
            list_recover_back_index.push(index_back);
        }
        list_recover_back_index.sort();

        // compute the two index by extending the list of index to left and right by 1
        // Example: responsible for [3,4,5,6], all backends [0,1,2,3,4,5,6,7,8] will have 2 and 7
        //                          [5,6,7,8], all backends [0,..,8] will have 4 and 0
        //                          [0,1,2,8], all backends [0,..,8] will have 7 and 3

        let (l_, r_) = find_lr(list_recover_back_index.clone());
        let len = list_all_back_hash.len() as i32;
        let mut l = list_recover_back_index.clone()[l_] as i32;
        let mut r = list_recover_back_index.clone()[r_] as i32;
        l = (l - 1 + len) % len;
        r = (r + 1) % len;

        if PRINT_DEBUG >= 3 && self.keeper_addr == DEBUG_KEEPER.to_string() {
            println!("====In update_back_clock in keeper {}", self.keeper_addr);
            println!(
                "====list_recover_back_index: {:?}",
                list_recover_back_index.clone()
            );
            println!("====l_ {}\tr_ {}", l_, r_);
            println!("====l {}\tr {}", l, r);
        }

        // if append l and r to list, sort list, and remove duplicate
        list_recover_back_index.push(l as usize);
        list_recover_back_index.push(r as usize);
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
            .unwrap_or_else(|x| x % keepers_len) as i32;
        let len = keepers_len as i32;
        let index_prev = (index_self - 1 + len) % len;
        self.prev_keeper = self.keepers.clone()[index_prev as usize].clone();
    }

    // called after check_prev_keeper changes keeper
    // updates list_back_recover, list_back_clock based on self.prev_keeper
    pub fn update_back_recover(&mut self) {
        // use a two keeper vec [self, self.prev_keeper], see which backends should be handled by self and not already in self.list_back_recover
        let mut pair_keeper = vec![
            ChordObject {
                addr: self.keeper_addr.clone(),
                hash: bin_aware_cons_hash(&self.keeper_addr.clone()),
            },
            ChordObject {
                addr: self.prev_keeper.clone(),
                hash: bin_aware_cons_hash(&self.prev_keeper.clone()),
            },
        ];
        pair_keeper.sort_by(|x, y| x.hash.cmp(&y.hash));
        assert_eq!(pair_keeper[0].hash < pair_keeper[1].hash, true);
        let pair_keeper_hash: Vec<u64> = pair_keeper.iter().map(|x| x.hash).collect();
        assert_eq!(pair_keeper[0].hash == pair_keeper_hash[0], true);
        assert_eq!(pair_keeper[1].hash == pair_keeper_hash[1], true);

        let index_self = pair_keeper_hash
            .binary_search(&bin_aware_cons_hash(&self.keeper_addr.clone()))
            .unwrap_or_else(|x| x % pair_keeper_hash.len());
        for back_obj in self.list_all_back_chord.clone() {
            let mut index_keeper = pair_keeper_hash
                .binary_search(&back_obj.hash)
                .unwrap_or_else(|x| x % pair_keeper_hash.len());
            if index_keeper > pair_keeper_hash.len() - 1 {
                index_keeper = 0; // when index_keeper is larger than the index of the last keeper, this backend belongs to the first keeper
            }
            if index_keeper == index_self {
                // see if we already have it
                match self.find_back_recover(back_obj.addr.clone()) {
                    Some(_) => (),
                    None => {
                        // if we don't have it, add it to list_back_recover
                        self.add_back_recover(back_obj.addr.clone());
                    }
                }
            }
        }
        self.update_back_clock();
    }

    // when any keeper between self and self.prev_keeper becomes alive, update list_back_recover, list_back_clock, prev_keeper, prev_alive_keeper_list_back
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
                if PRINT_DEBUG == 3 {
                    println!(
                        "----Handle join of keeper {}\t in keeper {}\n ----index_self {}, index_prev {}\nself.keeper: ",
                        added_keeper.clone(),
                        self.keeper_addr.clone(),
                        index_self, index_prev
                    );
                    for keeper in self.keepers.clone() {
                        println!("----{}", keeper.clone());
                    }
                }
                // We found an added keeper. Need to update self's fields so that backends that should be handled by
                let mut list_remove_back: Vec<String> = Vec::new();

                // find all backends that added_keeper is responsible for, using list_triple_hash, which
                //      is a sorted and no-duplicate vector of keeper's hash: [self.hash(), added_keeper.hash(), prev_keeper.hash()]
                //      and length is 2 or 3 (doesn't matter)
                let mut list_triple_chord: Vec<ChordObject> = vec![
                    ChordObject {
                        addr: self.keeper_addr.clone(),
                        hash: bin_aware_cons_hash(&self.keeper_addr.clone()),
                    },
                    ChordObject {
                        addr: added_keeper.clone(),
                        hash: bin_aware_cons_hash(&added_keeper.clone()),
                    },
                    ChordObject {
                        addr: self.prev_keeper.clone(),
                        hash: bin_aware_cons_hash(&self.prev_keeper.clone()),
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
                // let mut new_backends: Vec<String> = Vec::new();
                // for back_obj in self.list_back_recover.clone() {
                //     match list_remove_back
                //         .clone()
                //         .into_iter()
                //         .find(|x| *x == back_obj.back_addr.clone())
                //     {
                //         Some(_) => (),
                //         None => {
                //             // didn't find back in list_remove_back, meaning this back should be in new self.backends
                //             new_backends.push(back_obj.back_addr.clone());
                //         }
                //     }
                // }

                self.deduct_back_recover(list_remove_back.clone());
                self.prev_keeper = added_keeper.clone();
                // TODO: do we still need prev_alive_keeper_list_back?

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

    // prints this keeper's status. Including its addr, prev_keeper, back_ends responsible for, back_ends it syncs clock, special task, time passed
    pub fn print_keeper_status(&mut self) {
        let elapsed = match self.debug_timer.elapsed() {
            Ok(e) => e,
            Err(err) => {
                println!(
                    "Warning in print_keeper_status: now.elapsed() has error {}",
                    err
                );
                return;
            }
        };
        println!("=========");
        println!(
            "Time elapsed:{}s{}ms\tKeeper_addr:{}\tPrev_keeper_addr:{}\t",
            elapsed.as_secs(),
            elapsed.as_millis() - 1000 * (elapsed.as_secs() as u128),
            self.keeper_addr,
            self.prev_keeper
        );
        print!(
            "\tResponsible for {} backends:",
            self.list_back_recover.len()
        );
        for back in self.list_back_recover.clone() {
            if back.liveness {
                print!("({}, Alive)\t", back.back_addr);
            } else {
                print!("({}, Dead )\t", back.back_addr);
            }
        }
        println!();
        print!("\tSync clock for {} backends:", self.list_back_clock.len());
        for back in self.list_back_clock.clone() {
            print!("({})\t", back);
        }
        println!();
        println!("=========");
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
        log::info!("Starting heartbeat");
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
                        if PRINT_DEBUG == 3 {
                            println!(
                                "----Handle join of backend {}\t in keeper {}",
                                back.back_addr.clone(),
                                self.keeper_addr.clone()
                            );
                        }
                        self.set_back_recover_liveness(back.back_addr.clone(), true);

                        log::info!("found server recovered.");
                        self.join(&addr.clone()).await;
                    }
                }
                None => {
                    // this backend is in list_back_clock but not in list_back_recover
                    // no need to do anything to it
                }
            }
        }

        // (3) check if its previous alive keeper is now dead
        // TODO: ask Qizeng if this function also handles the update of self.backends, and migration of dead previous keeper's backends
        if self.keeper_addr != self.prev_keeper {
            let old_prev_keeper = self.prev_keeper.clone();
            self.check_prev_keeper().await;
            if old_prev_keeper == self.prev_keeper.clone() {
                self.update_back_recover();
            }
        }

        // (4) check if any keeper between it and its previous alive keeper is now added
        //      if so, update prev_keeper, and all fields related to backends
        self.check_new_keeper_between().await;

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
                                if PRINT_DEBUG == 3 {
                                    println!(
                                        "----Handle death of backend {}\t in keeper {}",
                                        addr.clone(),
                                        self.keeper_addr.clone()
                                    );
                                }
                                // this backend is really newly dead
                                self.set_back_recover_liveness(addr.clone(), false);
                                log::info!("found server dead.");
                                self.migrate(&addr.clone()).await;
                            }
                        };
                    }
                }
                None => {
                    // this backend is in list_back_clock but not in list_back_recover
                    // no need to do anything to it
                }
            }
        }

        if PRINT_DEBUG >= 1 && DEBUG_KEEPER == self.keeper_addr {
            self.print_keeper_status();
        }

        return Ok(());
    }
}

const DEBUG_KEEPER: &str = "http://127.0.0.1:34305";
