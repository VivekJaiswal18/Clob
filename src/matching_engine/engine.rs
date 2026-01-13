use ringbuf::traits::Consumer;
use ringbuf::{HeapCons};
use crate::output::Depth;
use crate::orderbook::{Order, OrderBook};
use crate::{event::OrderEvent};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::error;

 pub async fn matching_loop(mut order_cons: HeapCons<OrderEvent>, depth_snapshot: Arc<RwLock<Depth>>){
   let mut orderbook = OrderBook::new();
   let mut idle_iterations: u32 = 0;
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
                  if let Err(e) = orderbook.matching_order(Order { order_id, price, quantity, side }){error!("Order matching error: {}", e)};
               }
               OrderEvent::DeleteOrder { order_id }=>{
                  if let Err(e) = orderbook.delete_order(order_id){error!("Order deletion error: {}", e)};
               }
         }
         idle_iterations+=1;
         if idle_iterations.is_multiple_of(100){
            update_depth_cache(&mut orderbook, &depth_snapshot);
         }
      }
      None => {
         
         idle_iterations +=1;
         if idle_iterations ==1{
            update_depth_cache(&mut orderbook, &depth_snapshot);
         };
         if idle_iterations < 1000{
            std::hint::spin_loop();}
            else{
            tokio::task::yield_now().await;
         }
         idle_iterations =0;
      }
   }
   }
 }

pub fn update_depth_cache(orderbook: &mut OrderBook, depth_snapshot: &Arc<RwLock<Depth>>){
   let depth = orderbook.get_depth( 20);
   let mut snapshot = depth_snapshot.write();
   snapshot.bids = depth.bids;
   snapshot.asks = depth.asks;
   snapshot.last_update_id = depth.last_update_id;
   
}
