use std::hint::spin_loop;
use std::mem;
use actix_web::{get, App, HttpServer, web::{Data}}; 
use tokio::sync::mpsc;
use ringbuf::{HeapRb, traits::{Split, Producer}};
use tokio::task::yield_now;
use std::sync::Arc;
use parking_lot::RwLock;
use orderbook::ORDER_ID;

use orderbook::route::{create_order, delete_order};
use orderbook::event::{OrderEvent};
use orderbook::matching_engine::matching_loop;
use orderbook::output::{Depth};

// mod input;
// mod output;
// mod route;
// mod event;
// mod matching_engine;
// mod orderbook;
// mod error;
// mod websocket;
// mod persist;
// mod engine_registry;

// pub static ORDER_ID: AtomicU64 = AtomicU64::new(1);

#[actix_web::main]
async fn main () -> std::io::Result<()>{

    let (order_tx, mut order_rx) = mpsc::unbounded_channel::<OrderEvent>();
    let sender = Arc::new(order_tx);

    let order_rb = HeapRb::<OrderEvent>::new(512);
    let (mut order_prod, order_cons) = order_rb.split();

    let depth_snapshot = Arc::new(RwLock::new(Depth{
        asks: vec![],
        bids: vec![],
        last_update_id: "0".to_string(),
    }));

    tokio::spawn(async move {
        let mut batch = Vec::with_capacity(1024);
        while let Some(first) = order_rx.recv().await{
            batch.push(first);
        while batch.len() < 256 {
            match order_rx.try_recv(){
                Ok(event)=> batch.push(event),
                Err(e)=> break,
            }
        };  //here only one disadvantage is, on low ops time, this may not be the ideal method and pushing orderEvent one by one to the ringbuffer would be better. 
        // order_prod.push(batch); check for reason
        let mut spin :u8 = 0;
        let batch_push = mem::take(&mut batch);
        // let s: OrderEvent;
        for s in mem::take(&mut batch){
            let mut value = order_prod.try_push(s);
        while value.is_err(){
            spin += 1;
            if spin < 50{
                spin_loop();
            }
            else{
                yield_now().await;
                spin = 0;
            }
        }
    }

    }
});

tokio::spawn(async move {
    matching_loop(
        order_cons,
        depth_snapshot,
        // "SOL-USDC".to_string(),
    ).await;
});

    HttpServer::new(move || {
        App::new()
        .app_data(Data::new(sender.clone()))
        .service(create_order)
        .service(delete_order)
        // .service(get_depth)
    })
    .bind("127.0.0.1:8080")?
    .workers(12)
    .run()
    .await
} 