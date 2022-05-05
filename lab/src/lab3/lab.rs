use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};

// for binstorage
use crate::lab3::{binstorage::BinStorageClient, binstorage::calculate_hash, client::StorageClient, front::FrontServer, keeperserver::KeeperServer};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tonic::transport::Server as trServer;
use tribbler::rpc::trib_storage_server as rs;
use tribbler::storage::Storage;

// for keeper
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use tokio::time;
use tribbler::rpc;
use tribbler::rpc::trib_storage_client::TribStorageClient;

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
    let mut this_keeper = KeeperServer {
                keeper_addr: keeper_addr,
                list_back_recover: Vec::new(),
                list_back_clock: Vec::new(),
            
                prev_alive_keeper_addr: "".to_string(), // this should be updated
                prev_alive_keeper_list_back: Vec::new(),
            };

    // set its backends_recover and backends_clock
    this_keeper.init_back_recover_and_clock(backs.clone(), keepers.clone());

    // set its previous keeper, no matter it is alive or not
    let mut vec_keeper : Vec<KeeperServer> = Vec::new();

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
