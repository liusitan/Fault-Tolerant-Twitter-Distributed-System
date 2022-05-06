use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, self},
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


pub async fn setup_backend(
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
    if !ready {
        return Err(Box::new(TribblerError::Unknown(
            "keeper failed to start".to_string(),
        )));
    }
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
// \RUST_BACKTRACE=full

fn backend_quick_hash(backs:&Vec<String>,name:&str) ->(Vec<String>,Vec<usize>,usize){

    let mut my_backs = backs.clone();
    my_backs.sort_by_key(|x| cons_hash(&("http://".to_string() + x)));
    let hashed_backs: Vec<u64> =   backs.iter().map(|x| cons_hash(&("http://".to_string() + x))).collect();
    let index = argsort(&hashed_backs);
    let mut b_hid = hashed_backs
            .binary_search(&cons_hash(&name.to_string()))
            .unwrap_or_else(|x| x % backs.len());
    return (my_backs.clone(),index,b_hid );
}
pub fn argsort<T: Ord>(data: &[T]) -> Vec<usize> {
    let mut indices = (0..data.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &data[i]);
    indices
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

async fn test_get_set_backupfail() -> TribResult<()> {
    
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    // let mut handles = vec![];
    // for i in 0..5{
    //     handles.push(handles_[index[i]]);
    // }
    let client = lab2::new_bin_client(backs).await?;
    let user_bin = client.bin(name).await?;
    user_bin.set(&KeyValue {
        key: "hello".to_string(),
        value: "world".to_string(),
    }).await?;
    shutdowns[index[(b_hid+1)%shutdowns.len()]].send(()).await?;

    // std::thread::sleep(Duration::from_secs(2));
    let val = user_bin.get(&"hello").await?;
    assert_eq!(val.is_some(), true);
    assert_eq!(val.unwrap(), "world".to_string());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

async fn test_get_set_primaryfail() -> TribResult<()> {
    
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec![
    "127.0.0.1:8000".to_string(),
    "127.0.0.1:8001".to_string(), 
    "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), 
    "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    // let mut handles = vec![];
    // for i in 0..5{
    //     handles.push(handles_[index[i]]);
    // }
    let client = lab2::new_bin_client(backs).await?;
    let user_bin = client.bin(name).await?;
    user_bin.set(&KeyValue {
        key: "hello".to_string(),
        value: "world".to_string(),
    }).await?;
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;

    // std::thread::sleep(Duration::from_secs(2));
    let val = user_bin.get(&"hello").await?;
    assert_eq!(val.is_some(), true);
    assert_eq!(val.unwrap(), "world".to_string());
    Ok(())
}






fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn pat(prefix: &str, suffix: &str) -> Pattern {
    Pattern {
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
    }
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_get_set() -> TribResult<()> {
//     let (client, _handle, _tx) = setup(None, None).await?;
//     assert_eq!(None, client.get("").await?);
//     assert_eq!(None, client.get("hello").await?);
//     Ok(())
// }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_set_overwrite() -> TribResult<()> {
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    let client_ = lab2::new_bin_client(backs).await?;
    let client = client_.bin(name).await?;

    client.set(&kv("h8liu", "run")).await?;
    assert_eq!(Some("run".to_string()), client.get("h8liu").await?);
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;

    client.set(&kv("h8liu", "Run")).await?;
    assert_eq!(Some("Run".to_string()), client.get("h8liu").await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_set_none() -> TribResult<()> {
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    let client_ = lab2::new_bin_client(backs).await?;
    let client = client_.bin(name).await?;    client.set(&kv("h8liu", "")).await?;
    assert_eq!(None, client.get("h8liu").await?);
    client.set(&kv("h8liu", "k")).await?;
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;

    assert_eq!(Some("k".to_string()), client.get("h8liu").await?);
    client.set(&kv("h8he", "something")).await?;
    shutdowns[index[(b_hid+1)%shutdowns.len()]].send(()).await?;

    assert_eq!(Some("something".to_string()), client.get("h8he").await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_keys() -> TribResult<()> {
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    let client_ = lab2::new_bin_client(backs).await?;
    let client = client_.bin(name).await?;
    let _ = client.set(&kv("h8liu", "1")).await?;
    let _ = client.set(&kv("h8he", "2")).await?;
    let keys = client.keys(&pat("h8", "")).await?;
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;

    let mut v = keys.0;
    v.sort();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0], "h8he");
    assert_eq!(v[1], "h8liu");
    assert_eq!(0, client.list_get("lst").await?.0.len());
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_list() -> TribResult<()> {
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    let client_ = lab2::new_bin_client(backs).await?;
    let client = client_.bin(name).await?;
    client.list_append(&kv("lst", "a")).await?;
    let l = client.list_get("lst").await?.0;
    assert_eq!(1, l.len());
    assert_eq!("a", l[0]);

    client.list_append(&kv("lst", "a")).await?;
    let l = client.list_get("lst").await?.0;
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;

    assert_eq!(2, l.len());
    assert_eq!("a", l[0]);
    assert_eq!("a", l[1]);
    assert_eq!(2, client.list_remove(&kv("lst", "a")).await?);
    assert_eq!(0, client.list_get("lst").await?.0.len());

    client.list_append(&kv("lst", "h8liu")).await?;
    shutdowns[index[(b_hid)%shutdowns.len()]].send(()).await?;
    client.list_append(&kv("lst", "h7liu")).await?;
    let l = client.list_get("lst").await?.0;
    assert_eq!(2, l.len());
    assert_eq!("h8liu", l[0]);
    assert_eq!("h7liu", l[1]);

    let l = client.list_keys(&pat("ls", "st")).await?.0;
    assert_eq!(1, l.len());
    shutdowns[index[(b_hid+1)%shutdowns.len()]].send(()).await?;

    let l = client.list_keys(&pat("z", "")).await?.0;
    assert_eq!(0, l.len());

    let l = client.list_keys(&pat("", "")).await?.0;
    assert_eq!(1, l.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_list_keys() -> TribResult<()> {
    let (cli1_handle, cli1_shutdown) = setup_backend(Some(&"127.0.0.1:8000"), None).await?;
    let (cli2_handle, cli2_shutdown) = setup_backend(Some(&"127.0.0.1:8001"), None).await?;
    let (cli3_handle, cli3_shutdown) = setup_backend(Some(&"127.0.0.1:8002"), None).await?;
    let (cli4_handle, cli4_shutdown) = setup_backend(Some(&"127.0.0.1:8003"), None).await?;
    let (cli5_handle, cli5_shutdown) = setup_backend(Some(&"127.0.0.1:8004"), None).await?;

    let mut backs = vec!["127.0.0.1:8000".to_string(), "127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string(),
    "127.0.0.1:8003".to_string(), "127.0.0.1:8004".to_string()];
    let name = "Alice";

    let (hashed_sorted_backs,index,b_hid)= backend_quick_hash(&backs,name);

    let shutdowns = vec![cli1_shutdown,cli2_shutdown,cli3_shutdown,cli4_shutdown,cli5_shutdown];
    // let mut shutdowns = vec![];
    // for i in 0..5{
    //     shutdowns.push(shutdowns_[index[i]].clone());
    // }
    let handles = vec![cli1_handle,cli2_handle,cli3_handle,cli4_handle,cli5_handle];
    let client_ = lab2::new_bin_client(backs).await?;
    let client = client_.bin(name).await?;    let _ = client.list_append(&kv("t1", "v1")).await?;
    let _ = client.list_append(&kv("t2", "v2")).await?;
    shutdowns[index[(b_hid+2)%shutdowns.len()]].send(()).await?;

    let r = client.list_keys(&pat("", "")).await?.0;
    assert_eq!(2, r.len());
    Ok(())
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_awaited() -> TribResult<()> {
//     let (_client, srv, _shut) = setup(None, None).await?;
//     tokio::time::sleep(Duration::from_secs(2)).await;
//     srv.abort();
//     let r = srv.await;
//     assert!(r.is_err());
//     assert!(r.unwrap_err().is_cancelled());
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_bad_address() -> TribResult<()> {
//     let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
//     let cfg = BackConfig {
//         addr: "^_^".to_string(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx),
//         shutdown: None,
//     };
//     let handle = spawn_back(cfg);
//     if let Ok(ready) = rx.recv_timeout(Duration::from_secs(1)) {
//         if ready {
//             panic!("server should not have sent true ready signal");
//         }
//     };
//     let r = handle.await;
//     assert!(r?.is_err());
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_store_before_serve() -> TribResult<()> {
//     let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
//     let store = MemStorage::default();
//     store.set(&kv("hello", "hi")).await?;
//     let cfg = BackConfig {
//         addr: DEFAULT_HOST.to_string(),
//         storage: Box::new(store),
//         ready: Some(tx),
//         shutdown: None,
//     };
//     let _handle = spawn_back(cfg);
//     let ready = rx.recv_timeout(Duration::from_secs(1))?;
//     if !ready {
//         panic!("failed to start")
//     }
//     let client = lab1::new_client(format!("http://{}", DEFAULT_HOST).as_str()).await?;
//     assert_eq!(Some("hi".to_string()), client.get("hello").await?);
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_multi_serve() -> TribResult<()> {
//     let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
//     let cfg = BackConfig {
//         addr: DEFAULT_HOST.to_string(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx.clone()),
//         shutdown: None,
//     };
//     let cfg2 = BackConfig {
//         addr: "127.0.0.1:3001".to_string(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx.clone()),
//         shutdown: None,
//     };
//     spawn_back(cfg);
//     spawn_back(cfg2);
//     let ready =
//         rx.recv_timeout(Duration::from_secs(2))? && rx.recv_timeout(Duration::from_secs(2))?;
//     if !ready {
//         panic!("failed to start")
//     }
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_clock() -> TribResult<()> {
//     let (client, _srv, _shut) = setup(None, None).await?;
//     assert_eq!(2999, client.clock(2999).await?);
//     assert_eq!(3000, client.clock(0).await?);
//     assert_eq!(3001, client.clock(2999).await?);
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_spawn_same_addr() -> TribResult<()> {
//     let addr = DEFAULT_HOST.to_string();
//     let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
//     let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
//     let cfg = BackConfig {
//         addr: addr.clone(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx.clone()),
//         shutdown: Some(shut_rx),
//     };
//     let handle = spawn_back(cfg);
//     assert_eq!(true, rx.recv_timeout(Duration::from_secs(2))?);
//     let _ = shut_tx.send(()).await;
//     let _ = handle.await;
//     thread::sleep(Duration::from_millis(500));
//     let cfg = BackConfig {
//         addr: DEFAULT_HOST.to_string(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx),
//         shutdown: None,
//     };
//     let _ = spawn_back(cfg);
//     assert_eq!(true, rx.recv_timeout(Duration::from_secs(2))?);

//     let client = lab1::new_client(format!("http://{}", addr.clone()).as_str()).await?;
//     client.set(&kv("hello", "hi")).await?;
//     assert_eq!(Some("hi".to_string()), client.get("hello").await?);
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_back_spawn_new_storage() -> TribResult<()> {
//     let host = format!("127.0.0.1:{}", rand_port());
//     let (tx, rx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
//     let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
//     let cfg = BackConfig {
//         addr: host.clone(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx.clone()),
//         shutdown: Some(shut_rx),
//     };
//     let handle = spawn_back(cfg);
//     assert_eq!(true, rx.recv_timeout(Duration::from_secs(2))?);
//     let client = lab1::new_client(format!("http://{}", host).as_mut()).await?;
//     client.set(&kv("hello", "hi")).await?;
//     let _ = shut_tx.send(()).await?;
//     let _ = handle.await;
//     tokio::time::sleep(Duration::from_millis(500)).await;
//     let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
//     let cfg = BackConfig {
//         addr: host.clone(),
//         storage: Box::new(MemStorage::default()),
//         ready: Some(tx),
//         shutdown: Some(shut_rx),
//     };
//     let _ = spawn_back(cfg);
//     assert_eq!(true, rx.recv_timeout(Duration::from_secs(2))?);
//     assert_eq!(None, client.get("hello").await?);
//     let _ = shut_tx.send(()).await;
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_concurrent_cli_ops() -> TribResult<()> {
//     let (client, _srv, _shut) = setup(None, None).await?;
//     let client = Arc::new(client);
//     let mut handles = vec![];
//     for _ in 0..5 {
//         let addr = format!("http://{}", DEFAULT_HOST);
//         let jh = tokio::spawn(async move {
//             let client = match lab1::new_client(&addr).await {
//                 Ok(c) => c,
//                 Err(e) => return Err(TribblerError::Unknown(e.to_string())),
//             };
//             for _ in 0..10 {
//                 if let Err(e) = client.list_append(&kv("lst", "item")).await {
//                     return Err(TribblerError::Unknown(e.to_string()));
//                 };
//             }
//             Ok(())
//         });
//         handles.push(jh);
//     }
//     for handle in handles {
//         let res = handle.await;
//         assert!(res.is_ok());
//     }
//     assert_eq!(50, client.list_get("lst").await?.0.len());
//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// async fn test_shutdown() -> TribResult<()> {
//     let (client, srv, shutdown) = setup(None, None).await?;
//     assert!(client.set(&kv("hello", "hi")).await?);
//     let _ = shutdown.send(()).await;
//     let r = srv.await.unwrap();
//     assert!(r.is_ok());
//     match client.get("hello").await {
//         Ok(v) => panic!(
//             "uh oh..somehow the client still completed this request: {:?}",
//             v
//         ),
//         Err(_) => (),
//     };
//     Ok(())
// }
