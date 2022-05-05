use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};

// for binstorage
use super::{
    binstorage::BinStorageClient, client::StorageClient, front::FrontServer,
    keeperserver::serve_my_keeper_rpc, keeperserver::KeeperServer,
    keeperserver::KEEPER_INIT_DELAY_MS,
};
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
    let addr = match kc.addrs[kc.this].to_socket_addrs() {
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
        keeper_addr: keeper_addr,    //inited
        backends: Vec::new(),        //inited
        hashed_backends: Vec::new(), //inited
        keepers: Vec::new(),         //inited
        hashed_keepers: Vec::new(),  //inited

        list_back_recover: Vec::new(),   //inited
        list_back_clock: Vec::new(),     //inited
        list_all_back_chord: Vec::new(), //inited

        prev_keeper: "".to_string(),             //inited
        prev_alive_keeper_list_back: Vec::new(), // TODO: do we still need this?
    };

    // TODO: what if this keeper is restarted?

    // init its backends_recover and backends_clock
    this_keeper.init_back_recover_and_clock(backs.clone(), keepers.clone());

    // init prev_keeper, doesn't care whether the previous keeper is alive or not
    this_keeper.init_prev_keeper();

    // init RPCKeeperServer
    // This thread will send ready signal
    let (stop_rpc, mut stop_sign): (oneshot::Sender<()>, _) = oneshot::channel();
    tokio::spawn(serve_my_keeper_rpc(addr.clone(), kc.ready, stop_sign));

    // sleep for a while before we first start heartbeat
    // TODO: can we find a better way than this to solve the problem of uninitialized keepers?
    sleep(Duration::from_millis(KEEPER_INIT_DELAY_MS));

    // start the loop of heartbeat
    let mut dida = tokio::time::interval(time::Duration::from_secs(1)); // keeper syncs the clock of backends every 1000ms
    match kc.shutdown {
        Some(mut recv) => loop {
            tokio::select! {
                _ = dida.tick() => {
                    this_keeper.heart_beat().await;
                }
                _ = recv.recv() => {
                    if let Err(_) = stop_rpc.send(()) {
                        println!("Error during shutdown keeper: send failed");
                    }
                    break;
                }
            }
        },

        None => loop {
            tokio::select! {
                _ = dida.tick() => {
                    this_keeper.heart_beat().await;
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
