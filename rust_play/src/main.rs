// use std::{sync::RwLock, thread};

use tokio::sync::futures;
use tokio::sync::RwLock;

struct B{
    i:i32,
}
struct A{
    b:RwLock<Box<B>>,
}
impl A{
    async fn update(&self) {
        let mut bbox =  self.b.write().await;
        (*bbox) = Box::new(B{i:78});

    }
}
#[tokio::main]

async fn main(){
    let a1: A = A{b:RwLock::new(Box::new(B{i:24}))};
    let task = a1.update().await;
    let b =  a1.b.read().await;
    println!("{}",b.i);

    
}