use std::{
    process,
    sync::{
        mpsc::{self, Sender},
        Arc,
    },
    time::Duration,
};

use lab::{lab1, lab2};
use log::{error, info, warn, LevelFilter};
use nix::sys::wait::wait;
use nix::unistd::ForkResult::{Child, Parent};
use nix::unistd::{fork, getpid, getppid};
use tokio;
use tokio::join;
use tribbler::{addr, config::Config, err::TribResult, storage::MemStorage};
#[derive(Debug, Clone)]
pub enum ProcessType {
    Back,
    Keep,
}
pub async fn main(
    t: ProcessType,
    log_level: LevelFilter,
    cfg: String,
    _ready_addrs: Vec<String>,
    n: usize,

) -> TribResult<()> {
    env_logger::builder()
        .default_format()
        .filter_level(log_level)
        .init();
    let config = Arc::new(Config::read(Some(&cfg))?);

    println!("{:?}", config);
    let (tx, rdy) = mpsc::channel();

    let it = match t {
        ProcessType::Back => &config.backs,
        ProcessType::Keep => &config.keepers,
    };
    
                       run_srv(t.clone(), n, config.clone(), Some(tx.clone())).await;
    loop{}
    Ok(())
}

#[allow(unused_must_use)]
async fn run_srv(t: ProcessType, idx: usize, config: Arc<Config>, tx: Option<Sender<bool>>) {
    match t {
        ProcessType::Back => {
            let cfg = config.back_config(idx, Box::new(MemStorage::default()), tx, None);
            info!("starting backend on {}", cfg.addr);
            lab1::serve_back(cfg).await;
        }
        ProcessType::Keep => {
            let cfg = config.keeper_config(idx, tx, None).unwrap();
            info!("starting keeper on {}", cfg.addr());
            lab2::serve_keeper(cfg).await;
        }
    };
}
