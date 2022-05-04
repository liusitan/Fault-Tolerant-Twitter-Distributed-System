// use super::client::{Newhack, StorageClient};
use super::server::MyStorageServer;
use super::vbc_test::{Newhack, VirBinStorageClient};
use log;
/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
///
use std::io;
use std::net::Ipv4Addr;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num;
use std::sync::mpsc::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tonic::transport::server::Router;
use tonic::transport::Error;
use tonic::{transport::Server, Request, Response, Status};
use tribbler::rpc;
use tribbler::storage;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};
enum CliError {
    // IoError(io::Error),
    ParseError(num::ParseIntError),
}
impl From<num::ParseIntError> for CliError {
    fn from(error: num::ParseIntError) -> Self {
        CliError::ParseError(error)
    }
}
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let my_mem_storage = MyStorageServer::new(config.storage);
    let mut addrs_iter = config.addr.to_socket_addrs();
    let mut addrs_iter = match addrs_iter {
        Ok(x) => x,
        Err(e) => {
            if !config.ready.is_none() {
                let s = config.ready.unwrap();
                s.send(false);
            };
            return Err(Box::new(e));
        }
    };
    let mut addr = addrs_iter.next();

    let mut addr = addr.unwrap();
    let res: Result<(), Error>;
    // println!("serve_back 23 {:?}", config.ready.is_none());
    // let sh_channel = config.shutdown
    if config.shutdown.is_some() {
        // println!("serve_back 25");

        let mut sh_channel = config.shutdown.unwrap();
        // println!("serve_back 28");
        let res = Server::builder()
            .add_service(rpc::trib_storage_server::TribStorageServer::new(
                my_mem_storage,
            ))
            .serve_with_shutdown(addr, async {
                sh_channel.recv().await;
            });
        // println!("server_back 34");
        if !config.ready.is_none() {
            let sender = config.ready.unwrap();
            // println!("Sending true");

            sender.send(true);
            // println!("Sent true");
        };

        res.await;
    } else {
        let res = Server::builder()
            .add_service(rpc::trib_storage_server::TribStorageServer::new(
                my_mem_storage,
            ))
            .serve(addr);

        if !config.ready.is_none() {
            let sender = config.ready.unwrap();

            sender.send(true);
        };
        res.await;
    }
    // if res.is_err() {
    //     if !config.ready.is_none() {
    //         let s = config.ready.unwrap();
    //         s.send(false);
    //     }
    //     TribResult::from(Result::Err(Box::new(res.err().unwrap())))
    // } else {
    //     if !config.ready.is_none() {
    //         let s = config.ready.unwrap();
    //         s.send(true);
    //     }
    //     Ok(())
    // }
    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn storage::Storage>> {
    Ok(Box::new(VirBinStorageClient::neww(addr, "sitan").await))
}
