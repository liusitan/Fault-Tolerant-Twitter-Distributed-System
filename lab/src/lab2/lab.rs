use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};

// for binstorage
use crate::lab2::{binstorage::BinStorageClient, client::StorageClient, front::FrontServer};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tonic::transport::Server as trServer;
use tribbler::rpc::trib_storage_server as rs;
use tribbler::storage::Storage;

// for keeper
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

    let mut interval = time::interval(time::Duration::from_millis(800)); // keeper syncs the clock of backends every 800ms
    match kc.shutdown {
        Some(recv) => {
            // the following code is from https://users.rust-lang.org/t/wait-for-futures-in-loop/43007/3
            let (stop_read, mut time_to_stop): (oneshot::Sender<()>, _) = oneshot::channel();
            tokio::spawn(async move {
                block_fn(recv).await;
                if let Err(_) = stop_read.send(()) {
                    println!("something goes wrong during send");
                }
            });
            loop {
                interval.tick().await;
                tokio::select! {
                    _ = &mut time_to_stop => {
                        // exist if shutdown has received
                        return Ok(());
                    }
                    _ = sync_backs_clock(kc.backs.clone()) => {
                        // do our task normally
                    }
                };
            }
        }

        None => loop {
            interval.tick().await;
            sync_backs_clock(kc.backs.clone()).await;
        },
    }

    return Ok(());
}

pub async fn block_fn(mut recv: Receiver<()>) {
    recv.recv().await;
    return ();
}

async fn sync_backs_clock(backs: Vec<String>) -> TribResult<()> {
    let mut max_timestamp = 0;
    for back in backs.clone() {
        let mut client = TribStorageClient::connect(back.clone()).await?;
        let r = client
            .clock(rpc::Clock {
                timestamp: max_timestamp,
            })
            .await?;
        let t = r.into_inner().timestamp;
        if t > max_timestamp {
            max_timestamp = t;
        }
    }
    for back in backs {
        let mut client = TribStorageClient::connect(back.clone()).await?;
        let r = client
            .clock(rpc::Clock {
                timestamp: max_timestamp,
            })
            .await?;
    }
    return Ok(());
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
