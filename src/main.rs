use std::hint::spin_loop;
use std::sync::atomic::{AtomicI32, AtomicI64};
use std::{mem};
use orderbook::persist::ScyllaClient;
use rdkafka::producer::FutureProducer;
use actix_web::{App, HttpRequest, HttpResponse, Error, HttpServer, get, web::{self, Data}}; 
use actix_web_actors::ws;
use actix::{Addr, Actor};
use orderbook::kafka_worker::producer::{start_kafka_consumer, start_kafka_producer};
use orderbook::persist::{event::PersistEvent, worker::start_snapshot_persistance};
use tokio::{sync::mpsc, time::Instant};
use ringbuf::{HeapRb, traits::{Split, Producer}};
use tokio::task::yield_now;
use uuid::Uuid;
use std::sync::Arc;
use parking_lot::RwLock;
// use dotenv::dotenv;

use orderbook::{route::{create_order, delete_order, get_depth}, websocket::{MarketDataServer, client::WsSession}};
use orderbook::event::{OrderEvent};
use orderbook::matching_engine::matching_loop;
use orderbook::output::{Depth};


#[actix_web::main]
async fn main () -> std::io::Result<()>{

    dotenv::dotenv().ok();

    let (order_tx, mut order_rx) = mpsc::unbounded_channel::<OrderEvent>();
    let sender = Arc::new(order_tx);

    let order_rb = HeapRb::<OrderEvent>::new(524_288);
    let (mut order_prod, order_cons) = order_rb.split();

    let (persist_event_tx, persist_event_rx) = mpsc::channel::<PersistEvent>(10000);
    let persist_event_sender = Arc::new(persist_event_tx); 

    let depth_snapshot = Arc::new(RwLock::new(Depth{
        asks: vec![],
        bids: vec![],
        last_update_id: "0".to_string(),
    }));

    // actix_web::rt::spawn(async move {
    tokio::spawn(async move {
        // eprintln!("Batching task started");
        let mut batch = Vec::with_capacity(1024);
        // eprint!("this is the order_rx {:?}", order_rx);
        while let Some(first) = order_rx.recv().await{
            // eprintln!("batching order in batch the order is not getting till here {:?}", first);
            // eprintln!("batching order in batch the order is not getting till here {:?}", order_cons);
            batch.push(first);
        while batch.len() < 256 {
            match order_rx.try_recv(){
                Ok(event)=> batch.push(event),
                Err(e)=> break,
            }
        };  //here only one disadvantage is, on low ops time, this may not be the ideal method and pushing orderEvent one by one to the ringbuffer would be better. 
        // order_prod.push(batch); check for reason
    //     let mut spin :u8 = 0;
    //     let mut batch_push = mem::take(&mut batch);
    //     for s in mem::take(&mut batch_push){
    //         let mut value = order_prod.try_push(s);

    //     for event in batch.drain(..){
    //         let mut spin = 0;
    //     // while value.is_err(){
    //     while order_prod.try_push(event).is_err(){
    //         spin += 1;
    //         if spin < 50{
    //             spin_loop();
    //         }
    //         else{
    //             yield_now().await;
    //             spin = 0;
    //         }
    //     }
    // }

        for event in batch.drain(..){
            let mut order_event = event;
            let mut spin = 0;
            loop{
                match order_prod.try_push(order_event){
                    Ok(()) => {
                        // eprintln!("Pushing order to ringbuffer");
                        break},
                    Err(returned_order) =>{
                        order_event = returned_order;
                        spin += 1;
                        if spin < 50{
                            spin_loop();
                            // eprintln!("spinning loop pushing to ringbuf");
                        }
                        else{
                            yield_now().await;
                            spin = 0;
                            // eprintln!("yeilding pushing to ringbuf");
                        }
                    }  
                }
            }
        }

    }
});

{
    let depth_snapshot = depth_snapshot.clone();
    // let broadcast = broadcast.clone();
    let event_sender = persist_event_sender.clone();
    tokio::spawn(async move {
    matching_loop(
        order_cons,
        event_sender,
        depth_snapshot,
        // "SOL-USDC".to_string(),
    ).await;
    });
}

let producer: FutureProducer = rdkafka::config::ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .create()
    .unwrap();

{
    tokio::spawn(async move {
        start_kafka_producer(persist_event_rx, producer).await;
        // eprintln!("kafka producer started");
    });
}

let scylla = Arc::new(ScyllaClient::new().await);
let kafka_offset = Arc::new(AtomicI64::new(-1));
let kafka_partition = Arc::new(AtomicI32::new(-1));

{
    let scyllaclient = Arc::clone(&scylla);
    let kafka_offset = Arc::clone(&kafka_offset);
    let kafka_partition = Arc::clone(&kafka_partition);
    tokio::spawn(async move{
        // eprintln!("kafka consumer started in main.rs");
        start_kafka_consumer(scyllaclient, kafka_offset, kafka_partition).await;
})};

{
    let scyllaclient = Arc::clone(&scylla);
    let kafka_offset = Arc::clone(&kafka_offset);
    let kafka_partition = Arc::clone(&kafka_partition);
    let depth_snapshot = Arc::clone(&depth_snapshot);
    tokio::spawn(async move{
    start_snapshot_persistance(scyllaclient, depth_snapshot, kafka_offset, kafka_partition).await;
    })
};
    
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
        .app_data(Data::new(persist_event_sender.clone()))
        .service(create_order)
        .service(delete_order)
        .service(get_depth)
        .route("/ws", web::get().to(ws))
    })
    .bind("127.0.0.1:8081")?
    .workers(12)
    .run()
    .await
}