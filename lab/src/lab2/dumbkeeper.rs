use std::collections::HashSet;

use crate::lab1::client;
use tribbler::storage;
use tribbler::storage::KeyValue;
use tribbler::storage::List;
use tribbler::storage::Pattern;
use tribbler::storage::{KeyList, KeyString, Storage};
use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};
struct DumbKeeper {
    cfg: KeeperConfig,
    failed_back: HashSet<String>,
}
impl DumbKeeper {
    fn new(cfg: KeeperConfig) -> DumbKeeper {
        return DumbKeeper {
            cfg,
            failed_back: HashSet::new(),
        };
    }
    async fn clock_update(&self) -> TribResult<()> {
        let mut c = 0;
        let mut recent_failed_backs = String::new();
        for ip in &self.cfg.backs {
            let client1 = client::StorageClient::new(&("http://".to_string() + ip)).await?;
            match client1.clock(c).await {
                Ok(x) => {
                    c = x;
                }
                Err(_) => {
                    recent_failed_backs = ip.clone();
                }
            }
        }

        for ip in &self.cfg.backs {
            let client1 = client::StorageClient::new(&("http://".to_string() + ip)).await?;
            client1.clock(c + 1).await?;
        }
        self.migrate(recent_failed_backs).await?;
        return Ok(());
    }
    async fn mirgrate_from_x_to_y(&self, x: &str, y: &str) -> TribResult<()> {
        let clientx = client::StorageClient::new(x).await?;
        let clienty = client::StorageClient::new(x).await?;
        let keys = clientx
            .list_keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?
            .0;
        for key in &keys {
            let values = clientx.list_get(key).await?.0;
            for value in &values {
                clienty
                    .list_append(&KeyValue {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .await?;
            }
        }
        return Ok(());
    }
    async fn migrate(&self, recent_failed_back: String) -> TribResult<()> {
        let res = self.cfg.backs.binary_search(&recent_failed_back);
        let mut id = 0;
        match res {
            Ok(x) => id = x,
            _ => (),
        };
        let mut i = id;
        let mut middle: &str = "";
        let mut mi = 0;
        let mut prev: &str = "";
        let mut next: &str = "";
        while i != id {
            if (!self.failed_back.contains(&self.cfg.backs[i])) {
                middle = &self.cfg.backs[i];
                mi = i;
            }

            i += 1;
            i %= self.cfg.backs.len();
        }
        i = mi;
        while i != id {
            if !self.failed_back.contains(&self.cfg.backs[i]) {
                next = &self.cfg.backs[i];
            }

            i += 1;
            i %= self.cfg.backs.len();
        }
        i = id;
        while i != id {
            if !self.failed_back.contains(&self.cfg.backs[i]) {
                prev = &self.cfg.backs[i];
            }
            i -= 1;
            i %= self.cfg.backs.len();
        }
        self.mirgrate_from_x_to_y(middle, next).await?;
        self.mirgrate_from_x_to_y(prev, middle).await?;
        Ok(())
    }
}
