use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration,
};

use std::thread::sleep;
use lab::{self, lab1, lab2};
use lab::lab2::utility::cons_hash;
use log::LevelFilter;
use tokio::{sync::mpsc::Sender as MpscSender, task::JoinHandle};
use tribbler::{rpc, config::KeeperConfig};
use tribbler::{addr::rand::rand_port, rpc::trib_storage_client::TribStorageClient};

#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

const DEFAULT_HOST: &str = "127.0.0.1:3000";

async fn setup_backend(
    addr: Option<&str>,
    storage: Option<Box<dyn Storage + Send + Sync>>,
) -> TribResult<(JoinHandle<TribResult<()>>, MpscSender<()>)> {
    let _ = env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .try_init();
    let addr = match addr {
        Some(x) => x,
        None => DEFAULT_HOST,
    };
    let storage = match storage {
        Some(x) => x,
        None => Box::new(MemStorage::new()),
    };
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg = BackConfig {
        addr: addr.to_string(),
        storage: storage,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let handle = spawn_back(cfg);
    let ready = rx.recv_timeout(Duration::from_secs(5))?;
    if !ready {
        return Err(Box::new(TribblerError::Unknown(
            "back failed to start".to_string(),
        )));
    }
    // let client = lab1::new_client(format!("http://{}", addr).as_str()).await?;
    Ok((handle, shut_tx.clone()))
}

async fn setup_keeper(
    backend_addrs: Vec<String>,
    keeper_addrs: Vec<String>,
    idx: usize, // which keeper addr to run
    incarnation: u128
) -> TribResult<(JoinHandle<TribResult<()>>, MpscSender<()>)> {
    let _ = env_logger::builder()
        .default_format()
        .filter_level(LevelFilter::Info)
        .try_init();
        
    let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg = KeeperConfig {
        backs: backend_addrs,
        addrs: keeper_addrs,
        this: idx,
        id: incarnation,
        ready: Some(tx.clone()),
        shutdown: Some(shut_rx),
    };

    let handle = spawn_keep(cfg);
    let ready = rx.recv_timeout(Duration::from_secs(5))?;
    log::info!("Waiting for ready signal from keepers...");
    if !ready {
        return Err(Box::new(TribblerError::Unknown(
            "keeper failed to start".to_string(),
        )));
    }
    log::info!("Keepers are now ready!");
    Ok((handle, shut_tx.clone()))
}


fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab1::serve_back(cfg))
}

fn spawn_keep(cfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab2::serve_keeper(cfg))
}

async fn get_string_key_val_from_srv(srv: &String, key: &String) -> TribResult<Option<String>> {
    let conn = TribStorageClient::connect(srv.clone()).await;
    if conn.is_ok() {
        let mut client = conn.unwrap();
        let val = client.get(rpc::Key {
            key: key.clone()
        }).await;
        if val.is_err() {
            return Ok(None);
        } else {
            return Ok(Some(val.unwrap().into_inner().value));
        }
    }
    return Err(Box::new(TribblerError::RpcError("rpc connection failed".to_string())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_set() -> TribResult<()> {

    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()];
    let client = lab2::new_bin_client(backs).await?;
    let user_bin = client.bin("Alice").await?;
    user_bin.set(&KeyValue {
        key: "hello".to_string(),
        value: "world".to_string(),
    }).await?;
    let val = user_bin.get(&"hello").await?;
    assert_eq!(val.is_some(), true);
    assert_eq!(val.unwrap(), "world".to_string());
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_fault_tolerance_single_keeper() -> TribResult<()> {

    // start a cluster of 5 backends
    let (bk1_handle, bk1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (bk2_handle, bk2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (bk3_handle, bk3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (bk4_handle, bk4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (bk5_handle, bk5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;
    let mut backs = vec!["http://127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(), "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let keepers = vec!["127.0.0.1:8005".to_string()];
    backs.sort_by(|x, y| cons_hash(x).cmp(&cons_hash(y)));
    let hashed_backends:Vec<u64> = backs.iter().map(|x| cons_hash(x)).collect();

    // start the keeper
    let (kp_handle, kp_shutdown) = setup_keeper(backs.clone(), keepers, 0, 0).await?;

    // store the same key-value pairs in two adjacent backends
    let insert_pos = hashed_backends.binary_search(&cons_hash(&"hello".to_string())).unwrap_or_else(|x| x % hashed_backends.len());
    let insert_bk = &backs[insert_pos];

    // log::info!("primary set");
    let mut primary_client = TribStorageClient::connect(format!("http://{}", insert_bk.clone())).await?;
    primary_client.set(rpc::KeyValue {
        key: "hello".to_string(),
        value: "world".to_string(),
    }).await;

    // log::info!("backup set");
    let backup_bk = &backs[(insert_pos + 1) % backs.len()];
    let mut backup_client = TribStorageClient::connect(format!("http://{}", backup_bk.clone())).await?;
    backup_client.set(rpc::KeyValue {
        key: "hello".to_string(),
        value: "world".to_string(),
    }).await;

    log::info!("sending shutdown signal to {}...", insert_bk);

    // primary srv crash
    match insert_bk.as_str() {
        "127.0.0.1:8000" => { bk1_shutdown.send(()).await;},
        "127.0.0.1:8001" => { bk2_shutdown.send(()).await;},
        "127.0.0.1:8002" => { bk3_shutdown.send(()).await;},
        "127.0.0.1:8003" => { bk4_shutdown.send(()).await;},
        "127.0.0.1:8004" => { bk5_shutdown.send(()).await;},
        _ => {},
    };

    sleep(Duration::from_secs(5));

    let backup_srv_now = &backs[(insert_pos + 2) % backs.len()];
    let val_res = get_string_key_val_from_srv(&format!("http://{}", backup_srv_now), &"hello".to_string()).await;
    assert_eq!(val_res.is_ok(), true);
    let val = val_res.unwrap();
    assert_eq!(val.is_some(), true);
    assert_eq!(val.unwrap(), "world".to_string());
    Ok(())
}