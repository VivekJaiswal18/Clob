use std::hint::spin_loop;
use std::{mem};

use actix_web::{App, HttpRequest, HttpResponse, Error, HttpServer, get, web::{self, Data}}; 
use actix_web_actors::ws;
use actix::{Addr, Actor};
use tokio::{sync::mpsc, time::Instant};
use ringbuf::{HeapRb, traits::{Split, Producer}};
use tokio::task::yield_now;
use uuid::Uuid;
use std::sync::Arc;
use parking_lot::RwLock;

use orderbook::{route::{create_order, delete_order, get_depth}, websocket::{MarketDataServer, client::WsSession}};
use orderbook::event::{OrderEvent};
use orderbook::matching_engine::matching_loop;
use orderbook::output::{Depth};


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

{
    let depth_snapshot = depth_snapshot.clone();
    // let broadcast = broadcast.clone();
    tokio::spawn(async move {
    matching_loop(
        order_cons,
        depth_snapshot,
        // "SOL-USDC".to_string(),
    ).await;
    });
}

let market_server = MarketDataServer::new().start();
let market_data_server = web::Data::new(market_server);

async fn ws(req: HttpRequest, stream: web::Payload, server: web::Data<Addr<MarketDataServer>>)->Result<HttpResponse, Error>{
    ws::start(
        WsSession{
            id: Uuid::nil(),
            hb: Instant::now(),
            server: server.get_ref().clone(),
        },
        &req,
        stream)
}

    HttpServer::new(move || {
        App::new()
        .app_data(Data::new(sender.clone()))
        .app_data(market_data_server.clone())
        .app_data(Data::new(depth_snapshot.clone()))
        .service(create_order)
        .service(delete_order)
        .service(get_depth)
        .route("/ws", web::get().to(ws))
    })
    .bind("127.0.0.1:8080")?
    .workers(12)
    .run()
    .await
} 