use ringbuf::traits::Consumer;
use actix_web::web::Data;
use ringbuf::{HeapCons};
use tokio::sync::mpsc;
use tokio::{sync::mpsc::Sender, time::Instant};
use crate::output::Depth;
use crate::orderbook::{ExecutedOrder, Order, OrderBook};
use crate::persist::event::PersistEvent;
use crate::{event::OrderEvent};
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;
use std::convert::TryFrom;
use tracing::error;


type EventSender = Arc<mpsc::Sender<PersistEvent>>;
// type EventSender = Arc<mpsc::UnboundedSender<PersistEvent>>;
//  pub async fn matching_loop(mut order_cons: HeapCons<OrderEvent>, persist_event_sender: Data<EventSender>, depth_snapshot: Arc<RwLock<Depth>>){
//  pub async fn matching_loop(mut order_cons: HeapCons<OrderEvent>, persist_event_sender: EventSender, depth_snapshot: Arc<RwLock<Depth>>, market: String){
 pub async fn matching_loop(mut order_cons: HeapCons<OrderEvent>, persist_event_sender: EventSender, depth_snapshot: Arc<RwLock<Depth>>){
   let mut orderbook = OrderBook::new();
   let mut idle_iterations: u32 = 0;
   let mut process_iterations: u32 = 0;
   let mut last_depth_update = Instant::now();
   loop{
   match order_cons.try_pop(){
      Some(order)=>{
         idle_iterations = 0;

         match order{
            OrderEvent::NewOrder { 
               order_id, 
               price, 
               side, 
               quantity } =>{
                  // if let Err(e) = orderbook.matching_order(Order { order_id, price, quantity, side }){error!("Order matching error: {}", e)};
                  // eprint!("order matching perfectly");
                  // if let Err(e) = orderbook.matching_order(Order { order_id, price, quantity, side }){error!("Order matching error: {}", e)}=>{
                  match to_persist_order(Order { order_id, price, quantity, side }){
                  Ok(ev)=>{if let Err(e) = persist_event_sender.send(ev).await{error!("Sending oders to kafka failed {:?}", e)}},
                  Err(e)=>{error!("Error converting types for kafka {:?}", e)}
                  }
                  match orderbook.matching_order(Order { order_id, price, quantity, side }){
                     Ok(mut executed_order_vec)=>{
                        // let mut executed_order : PersistEvent;
                        for executed_order in executed_order_vec.drain(..){
                           // let ev: PersistEvent = executed_order;
                           match to_persist_trade(executed_order){
                              // Ok(ev)=>{
                              Ok(ev)=>{
                                 if let Err(e) = persist_event_sender.send(ev).await{
                                    error!("Sending trades to kafka failed {:?}", e);
                                 }
                              },
                              Err(e)=> {
                                 // if let Err(e) = persist_event_sender.send
                                 error!("Sending trades to kafka failed {:?}", e)
                              }
                           }
                           
                        }
                     },   
                     Err(e)=> error!("Order Matching Failed {:?}", e)
                  }
                  // }
               }
               OrderEvent::DeleteOrder { order_id }=>{
                  // if let Err(e) = orderbook.delete_order(order_id){error!("Order deletion error: {}", e)};
                  match orderbook.delete_order(order_id){
                     Ok(())=> {
                        // if let Err(e) = persist_event_sender.send(order_id.into()).await{
                           // error!("Sending Delete Order to Kafka Failed {:?}", e)
                           error!("Sending Delete Order to Kafka Failed")
                           // }
                        }
                     Err(e)=> error!("Order Deletion Failed {:?}", e)
                     }
                  
               }
               // OrderEvent::Snapshot{}=>{}
         }
         process_iterations += 1;
         if process_iterations >=100 || last_depth_update.elapsed() >= Duration::from_millis(100){
            update_depth_cache(&mut orderbook, &depth_snapshot);
            process_iterations = 0;
            last_depth_update = Instant::now();
            // println!("depth snapshot {:?}", &depth_snapshot);
         }
      }
      None => {
         process_iterations = 0;
         idle_iterations +=1;
         if idle_iterations ==1{
            update_depth_cache(&mut orderbook, &depth_snapshot);
            // println!("depth snapshot {:?}", &depth_snapshot);
         };
         if idle_iterations < 1000{
            std::hint::spin_loop();}
            else{
            tokio::task::yield_now().await;
         }
         // idle_iterations =0;
      }
   }
   }
 }

pub fn update_depth_cache(orderbook: &mut OrderBook, depth_snapshot: &Arc<RwLock<Depth>>){
   // println!("update_depth_cache fn is used");
   let depth = orderbook.get_depth( 20);
   let mut snapshot = depth_snapshot.write();
   snapshot.bids = depth.bids;
   snapshot.asks = depth.asks;
   snapshot.last_update_id = depth.last_update_id;
   // println!("depth bids {:?}", snapshot.bids);
   // println!("depth asks {:?}", snapshot.asks);
   // println!("orderbook depth {:?}", orderbook.get_depth(2));
   
}

fn to_persist_trade(e: ExecutedOrder) -> Result<PersistEvent, &'static str> {
   Ok(PersistEvent::TradeExecuted {
       maker_id: i64::try_from(e.maker_id).map_err(|_| "maker_id overflow")?,
       taker_id: i64::try_from(e.taker_id).map_err(|_| "taker_id overflow")?,
       price: i64::try_from(e.price).map_err(|_| "price overflow")?,
       quantity: i64::try_from(e.quantity).map_err(|_| "quantity overflow")?,
       timestamp: e.timestamp,
   })
}

fn to_persist_order(e: Order)->Result<PersistEvent, &'static str>{
   Ok(PersistEvent::NewOrder { 
      order_id: i64::try_from(e.order_id).map_err(|_| "order_id overflow")?, 
      price: i64::try_from(e.price).map_err(|_| "price overflow")?, 
      quantity: i64::try_from(e.quantity).map_err(|_| "quantity overflow")?, 
      side: e.side
   })

}
