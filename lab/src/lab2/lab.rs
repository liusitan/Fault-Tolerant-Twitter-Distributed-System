use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};

// for binstorage
use crate::lab2::{binstorage::BinStorageClient, binstorage::calculate_hash, client::StorageClient, front::FrontServer};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tonic::transport::Server as trServer;
use tribbler::rpc::trib_storage_server as rs;
use tribbler::storage::Storage;

// for keeper
use tokio::time;
use std::time::{Duration, SystemTime};
use std::thread::sleep;
use tribbler::rpc;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use std::cmp::Ordering;
use std::cell::RefCell;

const PRINT_DEBUG: bool = true;

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    let mut http_backs: Vec<String> = Vec::new();
    for back in backs {
        http_backs.push("http://".to_string() + &back);
    }
    let bs = BinStorageClient {
        list_back: http_backs,
    };
    return Ok(Box::new(bs));
}

const CLOCK_TIMEOUT_MS :u64 = 300;

pub struct KeeperClient {
    keeper_addr: String, // addr of this keeper
    list_back_recover: Vec<BackendStatus>, // list of backends that this keeper is responsible for (controls migrate and join)
    list_back_clock: Vec<BackendStatus>, // list of backends that this keeper needs to sync the clock for. This list includes list_back

    prev_alive_keeper_addr: String, // addr of its previous alive keeper. This should be updated
    prev_alive_keeper_list_back: Vec<BackendStatus>, // list of backends that previous keeper is responsible for
                                         // this keeper will check their liveness every 10 seconds
}

fn main() {
    let list_all_back = vec!["123".to_string(),"456".to_string(),"789".to_string()];
    let list_all_keep = vec!["ab".to_string(),"cde".to_string(),"fg".to_string()];
    // put all backs and keepers to a list as ChordObject
    let mut list_chord_object :Vec<RefCell<ChordObject>> =Vec::new();

    for back in list_all_back {
        let o = ChordObject {
            hash: calculate_hash(&back.clone()), // hash = hash(addr)
            addr: back,
            prev: address::Nil,
        };
        list_chord_object.push(RefCell::new(o));
    }


    for keeper in list_all_keep {
        let o = ChordObject {
            hash: calculate_hash(&keeper.clone()), // hash = hash(addr)
            addr: keeper,
            prev: address::Nil,
        };
        list_chord_object.push(RefCell::new(o));
    }

    list_chord_object.sort();

    let mut prev_obj = address::Nil;
    {
        for i in 0..list_chord_object.len() {
            let mut o = list_chord_object[i].clone();
            o.borrow_mut().prev = prev_obj;
            prev_obj = address::address(Box::new(list_chord_object[i].clone()));
        }
        list_chord_object[0].clone().borrow_mut().prev = prev_obj;
    }
    

    println!("Loop1: length is {}", list_chord_object.len());
    for i in 0..list_chord_object.len() {
        match list_chord_object[i].clone().borrow_mut().prev {
            address::address(ref mut next_address) => {
                println!("\tobj hash: {}, obj addr: {}, prev_obj: {}", list_chord_object[i].clone().borrow_mut().hash, 
        list_chord_object[i].clone().borrow_mut().addr, next_address.into_inner().hash);
            }
            address::Nil => {
                println!("\tobj hash: {}, obj addr: {}, prev_obj: Nil", list_chord_object[i].clone().borrow_mut().hash, 
        list_chord_object[i].clone().borrow_mut().addr);
            }
        }
    }

    for i in 0..list_chord_object.len() {
        list_chord_object[i].clone().borrow_mut().addr = list_chord_object[i].clone().borrow_mut().addr.clone() + "ahaha";
    }
    
    println!("Loop2: length is {}", list_chord_object.len());
    for i in 0..list_chord_object.len() {
        println!("\tobj hash: {}, obj addr: {}", list_chord_object[i].clone().borrow_mut().hash, list_chord_object[i].clone().borrow_mut().addr);
    }
}

impl KeeperClient {
    // given all backends' addr, use hash to compute which backends should be put into list_back_recover and list_back_clock
    fn init_back_recover_and_clock(&mut self, list_all_back: Vec<String>, list_all_keep: Vec<String>) {
        // put all backs and keepers to a list as ChordObject
        let mut list_chord_object : Vec<RefCell<ChordObject>> = Vec::new();

        for back in list_all_back {
            let o = ChordObject {
                hash: calculate_hash(&back.clone()), // hash = hash(addr)
                addr: back,
                prev: address::Nil,
            };
            list_chord_object.push(RefCell::new(o));
        }

        for keeper in list_all_keep {
            let o = ChordObject {
                hash: calculate_hash(&keeper.clone()), // hash = hash(addr)
                addr: keeper,
                prev: address::Nil,
            };
            list_chord_object.push(RefCell::new(o));
        }

        list_chord_object.sort();

        let mut prev_obj = address::Nil;
        for obj in list_chord_object {
            obj.borrow_mut().prev = prev_obj;
            prev_obj = address::address(Box::new(obj));
        }
        list_chord_object[0].borrow_mut().prev = prev_obj;

    }

    fn add_back_recover(&mut self, back: String) {
        self.list_back_recover.push(BackendStatus {
            back_addr: back,
            liveness: true, // TODO: check if this is true or false
        });
    }

    fn add_back_clock(&mut self, back: String) {
        self.list_back_clock.push(BackendStatus {
            back_addr: back,
            liveness: true, // TODO: check if this is true or false
        });
    }

    fn add_prev_keeper(&mut self, keeper: KeeperClient) {
        self.prev_alive_keeper_addr = keeper.keeper_addr;
        self.prev_alive_keeper_list_back = keeper.list_back_recover.clone();
    }

    fn find_back_recover(&self, addr: String) -> Option<BackendStatus> {
        for back in self.list_back_recover.clone() {
            if back.back_addr == addr {
                return Some(back);
            }
        }
        return None;
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

    // hear_beat will sync the clock of all backends in list_back_clock
    // (1) if fail to connect to a backend, and it is in list_back_recover, and it was alive in the last round, then we wait for CLOCK_TIMEOUT_MS milliseconds and try again
    //      if it still fails, we observed a backend failure. We will run the migration procedure
    // (2) if suc to connect to a backend, and it is in list_back_recover, and it was dead in the last round
    //      we observed a backend join. We will run the join procedure
    // (3) check if its previous alive keeper is now dead
    // (4) check if any keeper between it and its previous alive keeper is now added
    async fn heart_beat(&mut self) -> TribResult<()> {

        let now = SystemTime::now();

        let mut max_timestamp = 0;
        let mut list_dead_addr: Vec<String> = Vec::new();
        let mut list_alive_addr: Vec<String> = Vec::new();
        for back in self.list_back_clock.clone() {
            // TODO: see how to reuse connection here and below
            let addr = back.back_addr;
            match TribStorageClient::connect(addr.clone()).await {
                Ok(mut client) => {
                    let r = client.clock(rpc::Clock {
                        timestamp: max_timestamp,
                    }).await?;
                    let t = r.into_inner().timestamp;
                    if t > max_timestamp {
                        max_timestamp = t;
                    }
                    list_alive_addr.push(addr.clone());
                },
                Err(e) => {
                    list_dead_addr.push(addr.clone());
                },
            };
            
        }

        if PRINT_DEBUG {
            println!("This keeper observe {} alive backends and {} dead backends", list_alive_addr.len(), list_dead_addr.len());
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
                    
                },
                None => println!("Error: impossible None result in step (2)"),
            }
        }


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
                                let r = client.clock(rpc::Clock {
                                    timestamp: max_timestamp,
                                }).await?;
                            },
                            Err(_) => {
                                // this backend is really newly dead
                                self.set_back_recover_liveness(addr.clone(), false);

                                // TODO: handle migration
                            },
                        };
                    }
                },
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

// BackendStatus is a struct that Keeper stores, recording each backend's addr and liveness
#[derive(Debug, Clone)]
pub struct BackendStatus {
    back_addr: String, // addr of the backend
    liveness: bool, // whether this backend is alive or not
}

// ChordObject is an object stored on the ring of Chord
#[derive(Debug, Clone)]
pub struct ChordObject {
    hash: u64, // hash = hash(addr)
    addr: String,
    prev: address,
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

#[derive(Debug, Clone)]
enum address {
    address(Box<RefCell<ChordObject>>),
    Nil,
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let addr = match kc.addrs[0].to_socket_addrs() {
        Ok(mut iterator) => match iterator.next() {
            Some(first_addr) => first_addr,
            None => {
                match kc.ready {
                    Some(send) => {
                        let send_result = send.send(false);
                        match send_result {
                            Ok(()) => (),
                            Err(e) => (),
                        }
                    }
                    None => (),
                }
                return Err(Box::new(TribblerError::Unknown(
                    "Error during iterator of to_socket_addrs()".to_string(),
                )));
            }
        },

        Err(error) => {
            match kc.ready {
                Some(send) => {
                    let send_result = send.send(false);
                    match send_result {
                        Ok(()) => (),
                        Err(e) => (),
                    }
                }
                None => (),
            }
            return Err(Box::new(TribblerError::Unknown(
                "Error during to_socket_addrs()".to_string(),
            )));
        }
    };

    match kc.ready {
        Some(send) => {
            let send_result = send.send(true);
            match send_result {
                Ok(()) => (),
                Err(e) => (),
            }
        }
        None => (),
    }

    let mut dida = tokio::time::interval(time::Duration::from_secs(1)); // keeper syncs the clock of backends every 1000ms
    let backs: Vec<String> = kc
        .backs
        .clone()
        .iter()
        .map(|x| "http://".to_string() + x)
        .collect();

    let keepers: Vec<String> = kc
    .addrs
    .clone()
    .iter()
    .map(|x| "http://".to_string() + x)
    .collect();

    // init this keeper
    let keeper_addr = kc.addrs[kc.this].clone();
    let mut this_keeper = KeeperClient {
                keeper_addr: keeper_addr,
                list_back_recover: Vec::new(),
                list_back_clock: Vec::new(),
            
                prev_alive_keeper_addr: "".to_string(), // this should be updated
                prev_alive_keeper_list_back: Vec::new(),
            };

    // set its backends_recover and backends_clock
    this_keeper.init_back_recover_and_clock(backs.clone(), keepers.clone());

    // set its previous keeper, no matter it is alive or not
    let mut vec_keeper : Vec<KeeperClient> = Vec::new();

    // sleep for a while before we first start heartbeat
    
    // TODO: put keepers and backends to the chord ring

    // for addr in kc.addrs {
    //     vec_keeper.push(KeeperClient {
    //         keeper_addr: addr.clone(),
    //         list_back_recover: Vec::new(),
    //         list_back_clock: Vec::new(),
        
    //         prev_alive_keeper_addr: "".to_string(), // this should be updated
    //         prev_alive_keeper_list_back: Vec::new(),
    //     })
    // }
    // let N = vec_keeper.len();
    // let mut rng = rand::thread_rng();
    // for back in kc.backs {
    //     let mut vec_index_keeper : Vec<usize> = Vec::new();
    //     while vec_index_keeper.len() < 3 {
    //         let index_keeper = rng.gen_range(0..N);
    //         match vec_index_keeper.into_iter().find(| &x| x == index_keeper) {
    //             None => vec_index_keeper.push(index_keeper),
    //             Some(_) => {},
    //         }
    //     }
    //     for i in vec_index_keeper {
    //         vec_keeper[i].backs.push(back.clone());
    //     }
    // }

    // // creates N threads, each thread for a keeper. It syncs the clock of the backs it is in charge of
    // for keeper in vec_keeper {
    //     let mut interval = time::interval(time::Duration::from_millis(800)); // keeper syncs the clock of backends every 800ms
    //     tokio::spawn(async move {
    //         interval.tick().await;
    //         keeper.Sync_clock().await;
    //     });
    // }
    match kc.shutdown {
        Some(mut recv) => loop {
            tokio::select! {
                _ = dida.tick() => {
                // sync_backs_clock(backs.clone()).await;
                }
                _ = recv.recv() => {
                    break;
                }
            }
        },

        None => loop {
            tokio::select! {
                _ = dida.tick() => {
                    // sync_backs_clock(backs.clone()).await;
                }
            }
        },
    }

    return Ok(());
}

pub async fn block_fn(mut recv: Receiver<()>) {
    recv.recv().await;
    return ();
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    let a = FrontServer {
        bin_storage: bin_storage,
    };
    return Ok(Box::new(a));
}
